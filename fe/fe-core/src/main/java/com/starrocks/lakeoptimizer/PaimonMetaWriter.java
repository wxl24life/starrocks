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

package com.starrocks.lakeoptimizer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.VarBinaryLiteral;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ExternalManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;

/**
 * Handles writing Paimon table metadata to Lake Optimizer entity tables.
 * Responsibilities include inserting new metadata and cleaning up expired snapshots.
 */
public class PaimonMetaWriter {
    private static final Logger LOG = LogManager.getLogger(PaimonMetaWriter.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";
    private static final int MAX_RETRY_TIMES = 3;
    private static final long RETRY_SLEEP_MS = 1000;

    private final String catalogName;
    private final List<List<Expr>> rowsBuffer;
    // Whether bucket_num in table schema is consistent with manifests
    private boolean bucketNumConsistent = true;

    public PaimonMetaWriter(String catalogName) {
        this.catalogName = catalogName;
        this.rowsBuffer = Lists.newArrayList();
    }

    /**
     * Write all metadata for a Paimon table into Lake Optimizer tables.
     * If write fails, cleanup partial data automatically.
     */
    public void writeTableMetadata(ConnectContext context,
                                   PaimonTable paimonTable,
                                   long snapshotId,
                                   TableSchema schema,
                                   String tableUuid,
                                   Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> groupedManifests) {
        try {
            writeTableMetadataInternal(context, paimonTable, snapshotId, schema, tableUuid, groupedManifests);
        } catch (Exception e) {
            // Cleanup partial data on write failure
            LOG.error("{} Write TableMetadata failed for {}.{}, cleaning up snapshot_id={}",
                    LOG_PREFIX, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), snapshotId, e);
            cleanupSnapshotData(context, paimonTable.getId(), snapshotId);
            throw new RuntimeException(e);
        }
        // Cleanup expired snapshots if write success
        cleanupExpiredSnapshots(context, paimonTable, snapshotId);
    }

    private void writeTableMetadataInternal(ConnectContext context,
                                            PaimonTable paimonTable,
                                            long snapshotId,
                                            TableSchema schema,
                                            String tableUuid,
                                            Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> groupedManifests)
            throws Exception {
        bucketNumConsistent = true;
        int batchSize = Config.lake_optimizer_refresh_partition_batch_size;
        int totalPartitions = groupedManifests.size();
        int totalBuckets = schema.numBuckets();
        if (batchSize <= 0 || totalPartitions <= batchSize) {
            batchSize = totalPartitions;
        }

        List<BinaryRow> allPartitions = new ArrayList<>(groupedManifests.keySet());
        int totalBatches = (totalPartitions > 0) ? (totalPartitions + batchSize - 1) / batchSize : 0;

        for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            int startIdx = batchIndex * batchSize;
            int endIdx = Math.min(startIdx + batchSize, totalPartitions);
            List<BinaryRow> batchPartitions = allPartitions.subList(startIdx, endIdx);

            Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> batchManifests =
                    filterManifestsByPartitions(groupedManifests, batchPartitions);

            // Process partition data for this batch
            Map<BinaryRow, String> batchPartitionNames = preparePartitionData(
                    paimonTable, snapshotId, batchManifests);
            flushData(context, LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME);
            rowsBuffer.clear();

            // Process file data for this batch
            prepareFileData(paimonTable, snapshotId, totalBuckets, batchPartitionNames, batchManifests);
            flushData(context, LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME);
            rowsBuffer.clear();
        }

        // If bucket number is not consistent, set bucket_num to 0 as a marker
        int bucketNum = bucketNumConsistent ? totalBuckets : 0;
        if (!bucketNumConsistent) {
            LOG.info("{} Bucket count mismatch detected for {}.{}, bucket pruning will be disabled",
                    LOG_PREFIX, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        }

        // Insert schema at the end
        prepareSchemaData(paimonTable, snapshotId, tableUuid, bucketNum);
        flushData(context, LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME);
        rowsBuffer.clear();
        LOG.info("{} Written metadata for {}.{}, snapshot_id={}",
                LOG_PREFIX, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), snapshotId);
    }

    /**
     * Filter groupedManifests to only include entries for the specified partitions.
     */
    private Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> filterManifestsByPartitions(
            Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> groupedManifests,
            List<BinaryRow> partitions) {
        Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> filtered = new LinkedHashMap<>();
        for (BinaryRow partition : partitions) {
            Map<Integer, List<ExternalManifestEntry>> bucketData = groupedManifests.get(partition);
            if (bucketData != null) {
                filtered.put(partition, bucketData);
            }
        }
        return filtered;
    }

    /**
     * Prepare schema data for insertion.
     */
    private void prepareSchemaData(PaimonTable paimonTable, long snapshotId, String tableUuid, int bucketNum) {
        List<Expr> row = Lists.newArrayList();

        // Primary key columns first (catalog_name, database_name, table_name)
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(paimonTable.getCatalogDBName()));
        row.add(new StringLiteral(paimonTable.getCatalogTableName()));

        // table_id
        row.add(new IntLiteral(paimonTable.getId(), Type.BIGINT));

        // table_uuid - Paimon table identity, used to detect table recreation
        row.add(new StringLiteral(tableUuid));

        // begin_snapshot - preserve from existing PaimonTable
        long beginSnapshot = paimonTable.getBeginSnapshot();
        if (beginSnapshot == -1L) {
            beginSnapshot = snapshotId;
        }
        row.add(new IntLiteral(beginSnapshot, Type.BIGINT));

        // end_snapshot - always update to current snapshot
        row.add(new IntLiteral(snapshotId, Type.BIGINT));

        // bucket_num - observed totalBuckets from manifest entries (0 = mixed/inconsistent)
        row.add(new IntLiteral(bucketNum, Type.INT));

        rowsBuffer.add(row);
    }

    /**
     * Prepare partition statistics data for insertion.
     */
    private Map<BinaryRow, String> preparePartitionData(PaimonTable paimonTable, long snapshotId,
                                                        Map<BinaryRow, Map<Integer,
                                                                List<ExternalManifestEntry>>> groupedManifests) {
        org.apache.paimon.table.Table nativeTable = paimonTable.getNativeTable();
        Options options = Options.fromMap(nativeTable.options());
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        options.get(PARTITION_DEFAULT_NAME),
                        nativeTable.rowType().project(nativeTable.partitionKeys()),
                        nativeTable.partitionKeys().toArray(new String[0]),
                        false);

        Map<BinaryRow, String> partitionNames = new HashMap<>();

        for (Map.Entry<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> entry : groupedManifests.entrySet()) {
            BinaryRow partition = entry.getKey();
            // Merge all ManifestEntries to get partition entry
            Collection<PartitionEntry> partitionEntryMap =
                    PartitionEntry.merge(
                            entry.getValue().values().stream()
                                    .flatMap(List::stream)
                                    .map(ExternalManifestEntry::manifestEntry)
                                    .collect(Collectors.toList()));

            // There will be only 1 partition
            PartitionEntry partitionEntry = partitionEntryMap.iterator().next();

            List<Expr> row = Lists.newArrayList();

            // table_id
            row.add(new IntLiteral(paimonTable.getId(), Type.BIGINT));

            // partition_name, will be empty string for un-partitioned table
            String partitionName = PartitionPathUtils.generatePartitionPath(computer.generatePartValues(partition));
            row.add(new StringLiteral(partitionName));

            // snapshot_id
            row.add(new IntLiteral(snapshotId, Type.BIGINT));
            
            partitionNames.put(partition, partitionName);

            // partition_values
            row.add(convertBinaryRowToLiteral(partition));

            // row_count
            row.add(new IntLiteral(partitionEntry.recordCount(), Type.BIGINT));

            // data_size
            row.add(new IntLiteral(partitionEntry.fileSizeInBytes(), Type.BIGINT));

            // file_count
            row.add(new IntLiteral(partitionEntry.fileCount(), Type.BIGINT));

            // min_key/max_key (not available from PartitionEntry)
            row.add(new NullLiteral());
            row.add(new NullLiteral());

            // last_file_creation_time
            row.add(new IntLiteral(partitionEntry.lastFileCreationTime(), Type.BIGINT));

            rowsBuffer.add(row);
        }
        
        return partitionNames;
    }

    /**
     * Prepare file statistics data for insertion.
     */
    private void prepareFileData(PaimonTable paimonTable,
                                long snapshotId,
                                int totalBuckets,
                                Map<BinaryRow, String> partitionNames,
                                Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> groupedManifests) {
        for (Map.Entry<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> partitionEntry :
                groupedManifests.entrySet()) {
            String partitionName = partitionNames.get(partitionEntry.getKey());

            for (Map.Entry<Integer, List<ExternalManifestEntry>> bucketEntry : partitionEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();

                for (ExternalManifestEntry entryWithDeletion : bucketEntry.getValue()) {
                    ManifestEntry manifestEntry = entryWithDeletion.manifestEntry();
                    DeletionFile deletionFile = entryWithDeletion.deletionFile();
                    DataFileMeta fileMeta = manifestEntry.file();
                    int totalBucketsInManifests = manifestEntry.totalBuckets();

                    if (bucketNumConsistent && totalBucketsInManifests != totalBuckets) {
                        bucketNumConsistent = false;
                    }

                    List<Expr> row = buildFileStatRow(paimonTable, snapshotId, partitionName, bucket,
                            totalBucketsInManifests, manifestEntry.kind().toByteValue(),
                            fileMeta, deletionFile);
                    rowsBuffer.add(row);
                }
            }
        }
    }

    /**
     * Build a single file statistics row.
     */
    private List<Expr> buildFileStatRow(PaimonTable paimonTable, long snapshotId, String partitionName,
                                        int bucket, int totalBuckets, byte fileKind,
                                        DataFileMeta fileMeta, DeletionFile deletionFile) {
        List<Expr> row = Lists.newArrayList();

        // Lake Optimizer specific fields
        row.add(new IntLiteral(paimonTable.getId(), Type.BIGINT));      // table_id
        row.add(new StringLiteral(partitionName));                       // partition_name
        row.add(new IntLiteral(bucket, Type.INT));                       // bucket
        row.add(new IntLiteral(snapshotId, Type.BIGINT));                // snapshot_id
        row.add(new IntLiteral(totalBuckets, Type.INT));                 // total_buckets
        row.add(new IntLiteral(fileKind, Type.TINYINT));                 // file_kind

        // DataFileMeta fields
        row.add(new StringLiteral(fileMeta.fileName()));                 // file_name
        row.add(new IntLiteral(fileMeta.fileSize(), Type.BIGINT));       // file_size
        row.add(new IntLiteral(fileMeta.rowCount(), Type.BIGINT));       // row_count

        // min_key / max_key 
        row.add(convertBinaryRowToLiteral(fileMeta.minKey()));
        row.add(convertBinaryRowToLiteral(fileMeta.maxKey()));

        // key stats
        row.add(convertBinaryRowToLiteral(fileMeta.keyStats().minValues()));
        row.add(convertBinaryRowToLiteral(fileMeta.keyStats().maxValues()));
        row.add(convertBinaryArrayToLiteral(fileMeta.keyStats().nullCounts()));

        // value stats
        row.add(convertBinaryRowToLiteral(fileMeta.valueStats().minValues()));
        row.add(convertBinaryRowToLiteral(fileMeta.valueStats().maxValues()));
        row.add(convertBinaryArrayToLiteral(fileMeta.valueStats().nullCounts()));

        // sequence numbers
        row.add(new IntLiteral(fileMeta.minSequenceNumber(), Type.BIGINT));  // min_sequence_number
        row.add(new IntLiteral(fileMeta.maxSequenceNumber(), Type.BIGINT));  // max_sequence_number

        row.add(new IntLiteral(fileMeta.schemaId(), Type.BIGINT));       // schema_id
        row.add(new IntLiteral(fileMeta.level(), Type.INT));             // level

        // extra_files
        row.add(convertStringListToArrayExpr(fileMeta.extraFiles()));  // extra_files

        row.add(new IntLiteral(fileMeta.creationTimeEpochMillis(), Type.BIGINT));  // creation_time

        // DataFileMeta fields - NULLABLE
        // delete_row_count
        if (fileMeta.deleteRowCount().isPresent()) {
            row.add(new IntLiteral(fileMeta.deleteRowCount().get(), Type.BIGINT));
        } else {
            row.add(new NullLiteral());
        }

        // embedded_file_index
        byte[] embeddedIndex = fileMeta.embeddedIndex();
        if (embeddedIndex != null && embeddedIndex.length > 0) {
            row.add(new VarBinaryLiteral(embeddedIndex));
        } else {
            row.add(new NullLiteral());
        }

        // file_source (0=APPEND, 1=COMPACT)
        if (fileMeta.fileSource().isPresent()) {
            row.add(new IntLiteral(fileMeta.fileSource().get().ordinal(), Type.TINYINT));
        } else {
            row.add(new NullLiteral());
        }

        // value_stats_cols
        List<String> valueStatsCols = fileMeta.valueStatsCols();
        if (valueStatsCols != null && !valueStatsCols.isEmpty()) {
            row.add(convertStringListToArrayExpr(valueStatsCols));
        } else {
            row.add(new NullLiteral());
        }

        // external_path
        if (fileMeta.externalPath().isPresent()) {
            row.add(new StringLiteral(fileMeta.externalPath().get()));
        } else {
            row.add(new NullLiteral());
        }

        // first_row_id
        Long firstRowId = fileMeta.firstRowId();
        if (firstRowId != null) {
            row.add(new IntLiteral(firstRowId, Type.BIGINT));
        } else {
            row.add(new NullLiteral());
        }

        // write_cols
        List<String> writeCols = fileMeta.writeCols();
        if (writeCols != null && !writeCols.isEmpty()) {
            row.add(convertStringListToArrayExpr(writeCols));
        } else {
            row.add(new NullLiteral());
        }

        // DeletionFile fields (NULLABLE)
        if (deletionFile != null) {
            row.add(new StringLiteral(deletionFile.path()));             // deletion_path
            row.add(new IntLiteral(deletionFile.offset(), Type.BIGINT)); // deletion_offset
            row.add(new IntLiteral(deletionFile.length(), Type.BIGINT)); // deletion_length
            row.add(new IntLiteral(deletionFile.cardinality(), Type.BIGINT)); // deletion_cardinality
        } else {
            row.add(new NullLiteral());  // deletion_path
            row.add(new NullLiteral());  // deletion_offset
            row.add(new NullLiteral());  // deletion_length
            row.add(new NullLiteral());  // deletion_cardinality
        }

        return row;
    }

    // ==================== Helper Methods ====================

    /**
     * Serialize a BinaryRow to a VarBinaryLiteral for storage in VARBINARY columns.
     *
     * <p>Uses Paimon's {@link SerializationUtils#serializeBinaryRow(BinaryRow)} which produces
     * a schemaless format: [4-byte arity] + [binary content]. This is the same serialization
     * used by Paimon internally for persisting BinaryRow in manifest files.
     *
     * @see org.apache.paimon.io.DataFileMetaSerializer#toRow — serializes minKey/maxKey
     * @see org.apache.paimon.manifest.ManifestEntrySerializer#convertTo — serializes partition
     * @see org.apache.paimon.stats.SimpleStats#toRow — serializes minValues/maxValues
     */
    private LiteralExpr convertBinaryRowToLiteral(BinaryRow row) {
        if (row != null) {
            byte[] bytes = SerializationUtils.serializeBinaryRow(row);
            return new VarBinaryLiteral(bytes);
        }
        return new NullLiteral();
    }

    /**
     * Serialize a BinaryArray to a VarBinaryLiteral for storage in VARBINARY columns.
     *
     * <p>Uses {@link BinaryArray#toBytes()} to export the raw underlying MemorySegment bytes.
     * On deserialization, the bytes are restored via
     * {@code BinaryArray.pointTo(MemorySegment.wrap(bytes), 0, length)}.
     */
    private LiteralExpr convertBinaryArrayToLiteral(BinaryArray array) {
        return new VarBinaryLiteral(array.toBytes());
    }

    private static final ArrayType STRING_ARRAY_TYPE = new ArrayType(ScalarType.createVarcharType(65530));

    private Expr convertStringListToArrayExpr(List<String> list) {
        if (list == null) {
            list = new ArrayList<>();
        }
        List<Expr> items = list.stream()
                .map(StringLiteral::new)
                .collect(java.util.stream.Collectors.toList());
        return new ArrayExpr(STRING_ARRAY_TYPE, items);
    }

    // ==================== Flush Methods ====================

    private void flushData(ConnectContext context, String tableName) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        List<String> columnNames = LakeOptimizerUtils
                .buildLakeOptimizerColumnRef(tableName)
                .stream()
                .map(com.starrocks.sql.ast.ColumnDef::getName)
                .collect(Collectors.toList());

        executeInsert(context, tableName, columnNames, rowsBuffer);
    }

    public void executeInsert(ConnectContext context, String tableName,
                              List<String> columnNames, List<List<Expr>> rows) throws Exception {
        if (rows.isEmpty()) {
            return;
        }

        QueryStatement queryStatement = new QueryStatement(new ValuesRelation(rows, columnNames));
        TableName fullTableName = new TableName(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME, tableName);
        InsertStmt insertStmt = new InsertStmt(fullTableName, queryStatement);
        insertStmt.setTargetColumnNames(columnNames);

        // Use simplified SQL for OrigStmt to avoid log bloat and memory overhead
        String simpleSql = String.format("INSERT INTO %s.%s (%d rows)",
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME, tableName, rows.size());
        insertStmt.setOrigStmt(new OriginStatement(simpleSql, 0));

        int retryCount = 0;
        do {
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                String errorMsg = context.getState().getErrorMessage();
                LOG.warn("{} Insert into {} failed: {} | rows={}", LOG_PREFIX, tableName, errorMsg, rows.size());

                if (StringUtils.contains(errorMsg, "Too many versions")) {
                    Thread.sleep(RETRY_SLEEP_MS);
                    retryCount++;
                } else {
                    throw new DdlException("Insert failed: " + errorMsg);
                }
            } else {
                AuditLog.getInternalAudit().info("{} Insert | QueryId={} | SQL={}",
                        LOG_PREFIX, DebugUtil.printId(context.getQueryId()), simpleSql);
                return;
            }
        } while (retryCount < MAX_RETRY_TIMES);

        throw new DdlException("Insert failed after " + MAX_RETRY_TIMES + " retries");
    }

    // ==================== Cleanup Methods ====================

    /**
     * Cleanup data for a specific snapshot.
     */
    public void cleanupSnapshotData(ConnectContext context, long tableId, long snapshotId) {
        LOG.info("{} Cleaning up snapshot data for table_id={}, snapshot_id={}",
                LOG_PREFIX, tableId, snapshotId);

        deletePartitionAndFileData(context,
                String.format("table_id = %d AND snapshot_id = %d", tableId, snapshotId));
    }

    /**
     * Cleanup expired snapshot data from entity tables.
     * 
     * @param context the connect context
     * @param paimonTable the Paimon table
     * @param newestSnapshotId the newest snapshot ID updated
     */
    private void cleanupExpiredSnapshots(ConnectContext context, PaimonTable paimonTable, long newestSnapshotId) {
        int retentionCount = Config.lake_optimizer_snapshot_retention_count;
        if (retentionCount <= 0) {
            return;
        }

        long tableId = paimonTable.getId();
        long beginSnapshot = paimonTable.getBeginSnapshot();
        long endSnapshot = paimonTable.getEndSnapshot();

        long thresholdSnapshot = newestSnapshotId - retentionCount + 1;
        // keep endSnapshot, because some queries might be using it
        if (thresholdSnapshot > endSnapshot) {
            thresholdSnapshot = endSnapshot;
        }

        // Only cleanup if threshold is greater than beginSnapshot
        if (thresholdSnapshot <= beginSnapshot) {
            LOG.debug("{} No expired snapshots to cleanup for table_id={}, threshold={}, begin={}",
                    LOG_PREFIX, tableId, thresholdSnapshot, beginSnapshot);
            return;
        }

        LOG.info("{} Cleaning up expired snapshots for table_id={}, range=[{}, {}], retaining >= {}",
                LOG_PREFIX, tableId, beginSnapshot, endSnapshot, thresholdSnapshot);

        try {
            deletePartitionAndFileData(context,
                    String.format("table_id = %d AND snapshot_id < %d", tableId, thresholdSnapshot));

            // Update begin_snapshot in table_schema after cleanup
            updateBeginSnapshot(context, paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName(), thresholdSnapshot);

            LOG.info("{} Expired snapshot cleanup completed for table_id={}, new begin_snapshot={}",
                    LOG_PREFIX, tableId, thresholdSnapshot);
        } catch (Exception e) {
            // Cleanup failure should not fail the refresh operation
            LOG.warn("{} Failed to cleanup expired snapshots for table_id={}: {}",
                    LOG_PREFIX, tableId, e.getMessage());
        }
    }

    /**
     * Clear all data for a table from entity tables.
     * Used when a table has been dropped
     */
    public void clearTableData(ConnectContext context, PaimonTable paimonTable) {
        long tableId = paimonTable.getId();
        String dbName = paimonTable.getCatalogDBName();
        String tableName = paimonTable.getCatalogTableName();

        LOG.info("{} Clearing all data for table: {}.{}.{} (table_id={}, table_uuid={})",
                LOG_PREFIX, catalogName, dbName, tableName, tableId, paimonTable.getUUID());

        deletePartitionAndFileData(context, String.format("table_id = %d", tableId));

        // Delete from table_schema by name (since table_id may be reused)
        String deleteSchemaSQL = String.format(
                "DELETE FROM %s.%s WHERE catalog_name = '%s' AND database_name = '%s' AND table_name = '%s'",
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME,
                catalogName, dbName, tableName);
        executeDelete(context, deleteSchemaSQL);

        LOG.info("{} Cleared all data for recreated table: {}.{}.{}", LOG_PREFIX, catalogName, dbName, tableName);
    }

    /**
     * Update begin_snapshot in table_schema after cleaning up old snapshots.
     */
    private void updateBeginSnapshot(ConnectContext context, String dbName, String tableName,
                                     long newBeginSnapshot) throws Exception {
        String updateSQL = String.format(
                "UPDATE %s.%s SET begin_snapshot = %d " +
                        "WHERE catalog_name = '%s' AND database_name = '%s' AND table_name = '%s' " +
                        "AND begin_snapshot < %d",
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME,
                newBeginSnapshot,
                catalogName, dbName, tableName,
                newBeginSnapshot);

        LOG.debug("{} Update begin_snapshot SQL: {}", LOG_PREFIX, updateSQL);

        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(updateSQL, context.getSessionVariable());
        parsedStmt.setOrigStmt(new OriginStatement(updateSQL, 0));

        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        context.setStartTime();
        executor.execute();

        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            String errorMsg = context.getState().getErrorMessage();
            LOG.warn("{} Update begin_snapshot failed: {} | SQL: {}", LOG_PREFIX, errorMsg, updateSQL);
            throw new DdlException("Update begin_snapshot failed: " + errorMsg);
        } else {
            AuditLog.getInternalAudit().info("{} Update | QueryId={} | SQL={}",
                    LOG_PREFIX, DebugUtil.printId(context.getQueryId()), updateSQL);
        }
    }

    /**
     * Delete from both partition_statistics and file_statistics with the given WHERE clause.
     */
    private void deletePartitionAndFileData(ConnectContext context, String whereClause) {
        String deletePartitionSQL = String.format("DELETE FROM %s.%s WHERE %s",
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME,
                whereClause);
        executeDelete(context, deletePartitionSQL);

        String deleteFileSQL = String.format("DELETE FROM %s.%s WHERE %s",
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME,
                whereClause);
        executeDelete(context, deleteFileSQL);
    }

    public void executeDelete(ConnectContext context, String sql) {
        LOG.debug("{} Delete SQL: {}", LOG_PREFIX, sql);
        try {
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            parsedStmt.setOrigStmt(new OriginStatement(sql, 0));

            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                String errorMsg = context.getState().getErrorMessage();
                LOG.warn("{} Delete failed: {} | SQL: {}", LOG_PREFIX, errorMsg, sql);
            } else {
                AuditLog.getInternalAudit().info("{} Delete | QueryId={} | SQL={}",
                        LOG_PREFIX, DebugUtil.printId(context.getQueryId()), sql);
            }
        } catch (Exception e) {
            LOG.error("{} Failed to execute delete SQL: {}", LOG_PREFIX, sql, e);
        }
    }
}
