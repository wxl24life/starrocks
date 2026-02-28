// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.lakeoptimizer.LakeOptimizerUtils;
import com.starrocks.lakeoptimizer.PaimonMetaWriter;
import com.starrocks.lakeoptimizer.cache.LakeOptimizerCacheManager;
import com.starrocks.lakeoptimizer.cache.TableCacheKey;
import com.starrocks.lakeoptimizer.cache.TableSchemaInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntryWithDeletionFile;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * LakeOptimizer-enabled Paimon catalog that extends {@link DefaultPaimonCatalog}.
 *
 */
public class CachingPaimonCatalog extends DefaultPaimonCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingPaimonCatalog.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";
    private static final String TRACE_PREFIX = "Paimon.LakeOptimizer.";

    /**
     * Tracks tables that are currently being refreshed on the Leader FE.
     * Prevents duplicate refresh when both async and sync refresh are triggered.
     */
    private static final Set<TableCacheKey> REFRESHING_TABLES = ConcurrentHashMap.newKeySet();

    private final LakeOptimizerCacheManager cacheManager;

    public CachingPaimonCatalog(String catalogName, Catalog paimonNativeCatalog) {
        super(catalogName, paimonNativeCatalog);
        this.cacheManager = GlobalStateMgr.getCurrentState().getLakeOptimizerCacheManager();
        LOG.info("{} Created CachingPaimonCatalog for catalog: {}", LOG_PREFIX, catalogName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        TableCacheKey key = new TableCacheKey(catalogName, dbName, tblName);
        return cacheManager.getTable(key, this::loadTable);
    }

    /**
     * Load table from native catalog and schema table.
     */
    private PaimonTable loadTable(TableCacheKey key) {
        String dbName = key.dbName;
        String tableName = key.tableName;

        PaimonTable table = (PaimonTable) super.getTable(dbName, tableName);

        if (!(table.getNativeTable() instanceof FileStoreTable)) {
            LOG.debug("{} {} is not FileStoreTable, using native mode", LOG_PREFIX, key);
            // TODO: these tables should not be cached
            return table;
        }

        TableSchemaInfo schemaInfo = cacheManager.getQueryService()
                .queryTableSchemaByName(catalogName, dbName, tableName);

        if (schemaInfo != null) {
            table.setId(schemaInfo.getTableId());
            table.setBeginSnapshot(schemaInfo.getBeginSnapshot());
            table.setEndSnapshot(schemaInfo.getEndSnapshot());
            table.setBucketNum(schemaInfo.getBucketNum());
            table.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.READY);
            return table;
        } else {
            // Table not yet refreshed to LakeOptimizer - first time access.
            // Return table with UNINITIALIZED state; 
            table.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.UNINITIALIZED);
            return table;
        }
    }

    @Override
    public Map<String, Partition> getPartitions(String databaseName, String tableName) {
        PaimonTable table = (PaimonTable) getTable(databaseName, tableName);
        // UNINITIALIZED and DISABLED tables should fallback to default.
        if (table.getLakeOptimizerMode() != PaimonTable.LakeOptimizerMode.READY) {
            LOG.debug("{} Table {}.{}.{} not ready (state={}), falling back to native partition retrieval",
                    LOG_PREFIX, catalogName, databaseName, tableName, table.getLakeOptimizerMode());
            return super.getPartitions(databaseName, tableName);
        }

        List<Partition> partitions = cacheManager.getPartitions(table.getId(), table.getEndSnapshot(), tableName);
        Map<String, Partition> result = new HashMap<>();
        if (partitions != null) {
            partitions.forEach(p -> result.put(p.getPartitionName(), p));
        }
        return result;
    }

    /**
     * Refresh Paimon table metadata into entity tables.
     * This method runs on Leader FE only
     */
    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        if (!(paimonTable.getNativeTable() instanceof FileStoreTable)) {
            LOG.warn("{} Table {}.{} is not a FileStoreTable, skip refresh",
                    LOG_PREFIX, srDbName, table.getName());
            return;
        }

        TableCacheKey tableKey = new TableCacheKey(catalogName,
                paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());

        // Acquire refresh lock
        if (!REFRESHING_TABLES.add(tableKey)) {
            LOG.info("{} Table {} is already being refreshed, skip duplicate refresh", LOG_PREFIX, tableKey);
            return;
        }

        ConnectContext context = null;
        try {
            context = LakeOptimizerUtils.buildConnectContext();
            context.setThreadLocalInfo();

            doRefreshTable(context, paimonTable, tableKey);
        } catch (Exception e) {
            LOG.error("{} refresh table {} failed", LOG_PREFIX, tableKey, e);
            throw new RuntimeException(e);
        } finally {
            REFRESHING_TABLES.remove(tableKey);
            if (context != null) {
                ConnectContext.remove();
            }
        }
    }

    private void doRefreshTable(ConnectContext context, PaimonTable paimonTable,
                                TableCacheKey tableKey) {
        Identifier identifier = new Identifier(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        PaimonMetaWriter writer = new PaimonMetaWriter(catalogName);

        // invalidate native cache first to get newest native table
        paimonNativeCatalog.invalidateTable(identifier);
        // table is dropped
        org.apache.paimon.table.FileStoreTable updateTable;
        try {
            updateTable = (FileStoreTable) paimonNativeCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            LOG.info("{} Table {} has been dropped, clearing entity data and invalidating cache",
                    LOG_PREFIX, identifier.getFullName());
            writer.clearTableData(context, paimonTable);
            invalidateTableCacheAndLog(tableKey);
            return;
        }

        TableSchemaInfo currentSchemaInfo = cacheManager.getQueryService()
                .queryTableSchemaByName(tableKey.catalogName, tableKey.dbName, tableKey.tableName);
        String updateUuid = updateTable.uuid();
        // table is dropped and created later with the same name
        boolean tableRecreated = false;
        if (currentSchemaInfo != null && !updateUuid.equals(currentSchemaInfo.getTableUuid())) {
            tableRecreated = true;
            LOG.info("{} Table {} has been recreated (UUID changed: stored={}, fresh={}), clearing old data",
                    LOG_PREFIX, identifier.getFullName(),
                    currentSchemaInfo.getTableUuid(), updateUuid);
            writer.clearTableData(context, paimonTable);
        }

        // snapshot already refreshed
        long snapshotId = updateTable.latestSnapshot().map(Snapshot::id).orElse(-1L);
        if (!tableRecreated && currentSchemaInfo != null && snapshotId <= currentSchemaInfo.getEndSnapshot()) {
            LOG.info("{} Snapshot {} already refreshed for {} (current end_snapshot={}), skip",
                    LOG_PREFIX, snapshotId, tableKey, currentSchemaInfo.getEndSnapshot());
            return;
        }

        if (currentSchemaInfo == null || tableRecreated) {
            // assign new table id if first time refresh
            paimonTable.setId(GlobalStateMgr.getCurrentState().getNextId());
            paimonTable.setBeginSnapshot(-1L);
            paimonTable.setEndSnapshot(-1L);
        }
        // collect new metadata and flush to entity tables
        TableSchema schema = updateTable.schema();
        Map<BinaryRow, Map<Integer, List<ManifestEntryWithDeletionFile>>> groupedManifests = new HashMap<>();
        // snapshot will be -1 for empty table
        if (snapshotId > 0) {
            groupedManifests = updateTable.listManifestEntriesWithDeletionFiles(snapshotId);
        }

        writer.writeTableMetadata(context, paimonTable, snapshotId, schema, updateUuid, groupedManifests);

        // update native table reference and invalidate cache across all FEs
        paimonTable.setPaimonNativeTable(updateTable);
        invalidateTableCacheAndLog(tableKey);
        LOG.info("{} Refreshed table {}, snapshot_id={}, table_recreated={}",
                LOG_PREFIX, tableKey, snapshotId, tableRecreated);
    }

    @Override
    public List<Split> getSplits(Table table, Map<String, Partition> partitions,
                                 Integer totalPartitionCount, long snapshotId, InnerTableScan scan) {
        PaimonTable paimonTable = (PaimonTable) table;
        if (paimonTable.getLakeOptimizerMode() == PaimonTable.LakeOptimizerMode.UNINITIALIZED) {
            LOG.info("{} Table {} not in entity table, trigger async refresh",
                    LOG_PREFIX, table.getName());
            GlobalStateMgr.getCurrentState().getLakeOptimizerRefreshManager()
                    .triggerAsyncRefresh(paimonTable);
            return super.getSplits(table, partitions, totalPartitionCount, snapshotId, scan);
        }
        if (paimonTable.getLakeOptimizerMode() == PaimonTable.LakeOptimizerMode.DISABLED) {
            return super.getSplits(table, partitions, totalPartitionCount, snapshotId, scan);
        }

        // Check if new snapshot needs async refresh
        long latestSnapshotId = paimonTable.getNativeTable().latestSnapshot()
                .map(s -> s.id())
                .orElse(-1L);

        if (latestSnapshotId > snapshotId) {
            LOG.info("{} Detected new snapshot for {}.{}.{}: entity={}, native={}, trigger async refresh",
                    LOG_PREFIX, catalogName, paimonTable.getCatalogDBName(), table.getName(), snapshotId, latestSnapshotId);
            GlobalStateMgr.getCurrentState().getLakeOptimizerRefreshManager().triggerAsyncRefresh(paimonTable);
        }

        // all partitions have been pruned
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }

        DataTableBatchScan batchScan = (DataTableBatchScan) scan;
        SnapshotReader snapshotReader = batchScan.getSnapshotReader();

        // Bucket pruning is disabled:
        // - Dynamic bucket mode (bucket = -1)
        // - Postpone bucket mode (bucket = -2)
        // - Bucket number is different between partitions
        Set<Integer> prunedBuckets = null;
        int bucketNum = paimonTable.getBucketNum();
        if (bucketNum > 0) {
            // use bucket_num from table_schema instead of from latest options
            // - if bucket number is changed, but rescale has not performed, use old bucket number
            // - once rescale is performed, bucket number in table_schema will be updated to new value

            // if no bucket key predicates provided, getPrunedBuckets returns null
            prunedBuckets = snapshotReader.getPrunedBuckets(bucketNum);
        }

        List<ManifestEntryWithDeletionFile> allManifestEntries = cacheManager.getManifestEntries(
                table.getId(), snapshotId, partitions, prunedBuckets, table.getName(),
                totalPartitionCount, table.isUnPartitioned());

        try (Timer ignored = Tracers.watchScope(EXTERNAL, TRACE_PREFIX + table.getName() + ".getSplitTime")) {
            return snapshotReader.readFromManifestEntriesWithDeletionFiles(snapshotId, allManifestEntries).splits();
        }
    }

    @Override
    public long getTableUpdateTime(PaimonTable table) {
        Long endSnapshot = table.getEndSnapshot();
        if (endSnapshot == null || endSnapshot <= 0) {
            return 0;
        }
        org.apache.paimon.table.Table nativeTable = table.getNativeTable();
        return nativeTable.snapshot(endSnapshot).timeMillis();
    }

    @Override
    public void traceCatalogMetrics(String prefix, PaimonMetricRegistry metricRegistry) {
        return;
    }

    private void invalidateTableCacheAndLog(TableCacheKey tableKey) {
        cacheManager.invalidateTable(tableKey.catalogName, tableKey.dbName, tableKey.tableName);
        GlobalStateMgr.getCurrentState().getEditLog().logInvalidateLakeOptimizerTableCache(tableKey);
    }
}
