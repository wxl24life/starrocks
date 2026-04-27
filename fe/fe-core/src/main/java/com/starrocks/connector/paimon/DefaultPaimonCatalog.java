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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonPartitionKey;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PaimonView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.KeysType.PRIMARY_KEYS;
import static com.starrocks.catalog.PaimonTable.FILE_FORMAT;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ColumnTypeConverter.toPaimonSchema;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

/**
 * Default implementation of {@link PaimonCatalog}.
 */
public class DefaultPaimonCatalog implements PaimonCatalog {
    private static final Logger LOG = LogManager.getLogger(DefaultPaimonCatalog.class);
    private static final String VIEW_DIALECTS_KEY = "starrocks";

    protected final String catalogName;
    protected final Catalog paimonNativeCatalog;

    public DefaultPaimonCatalog(String catalogName, Catalog paimonNativeCatalog) {
        this.catalogName = catalogName;
        this.paimonNativeCatalog = paimonNativeCatalog;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public Catalog getNativeCatalog() {
        return paimonNativeCatalog;
    }

    @Override
    public List<String> listDbNames() {
        return paimonNativeCatalog.listDatabases();
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException {
        try {
            paimonNativeCatalog.createDatabase(dbName, false, properties);
        } catch (Exception e) {
            LOG.error("Failed to create Paimon database {}.{}.", catalogName, dbName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException {
        try {
            paimonNativeCatalog.dropDatabase(dbName, false, isForceDrop);
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon database {}.{}.", catalogName, dbName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            List<String> tableIdentifiers = paimonNativeCatalog.listTables(dbName);
            List<String> viewIdentifiers = paimonNativeCatalog.listViews(dbName);
            if (!viewIdentifiers.isEmpty()) {
                tableIdentifiers.addAll(viewIdentifiers);
            }
            return tableIdentifiers;
        } catch (Exception e) {
            LOG.error("Failed to list Paimon tables {}.{}.", catalogName, dbName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        Schema.Builder schemaBuilder = toPaimonSchema(stmt.getColumns());

        KeysDesc keysDesc = stmt.getKeysDesc();
        if (null != keysDesc) {
            if (PRIMARY_KEYS != keysDesc.getKeysType()) {
                throw new DdlException("Paimon table does not support key type " + keysDesc.getKeysType().name());
            }
            schemaBuilder.primaryKey(keysDesc.getKeysColumnNames());
        }

        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        List<String> partitionColNames = partitionDesc == null ? Lists.newArrayList() :
                ((ListPartitionDesc) partitionDesc).getPartitionColNames();
        schemaBuilder.partitionKeys(partitionColNames);

        Map<String, String> properties = stmt.getProperties() == null ? new HashMap<>() : stmt.getProperties();
        properties.putIfAbsent(FILE_FORMAT, "parquet");
        schemaBuilder.options(properties);

        schemaBuilder.comment(stmt.getComment());

        Schema schema = schemaBuilder.build();

        try {
            paimonNativeCatalog.createTable(new Identifier(dbName, tableName), schema, false);
        } catch (Exception e) {
            LOG.error("Failed to create Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            throw new DdlException(e.getMessage());
        }
        return true;
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        Table paimonTable = getTable(stmt.getDbName(), stmt.getTableName());
        if (paimonTable == null) {
            return;
        }
        try {
            if (paimonTable.isPaimonView()) {
                paimonNativeCatalog.dropView(new Identifier(dbName, tableName), stmt.isForceDrop());
                return;
            }
            paimonNativeCatalog.dropTable(new Identifier(dbName, tableName), stmt.isForceDrop());
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        String partitionName = clause.getPartitionName();
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("dummy", partitionName);
        try {
            paimonNativeCatalog.dropPartitions(new Identifier(db.getOriginName(), table.getName()), List.of(partitionMap));
        } catch (Exception e) {
            LOG.error("Failed to drop Paimon table partition {}.{}.{}.", catalogName, db.getOriginName(), table.getName(), e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public Database getDb(String dbName) {
        try {
            paimonNativeCatalog.getDatabase(dbName);
            return new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
        } catch (Exception e) {
            LOG.error("Failed to get Paimon database {}.{}.", catalogName, dbName, e);
            return null;
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        org.apache.paimon.table.Table paimonNativeTable;
        try {
            paimonNativeTable = this.paimonNativeCatalog.getTable(identifier);
        } catch (Exception e) {
            LOG.error("Failed to get Paimon table {}.{}.{}.", catalogName, dbName, tblName, e);
            return getView(dbName, tblName);
        }
        List<DataField> fields = paimonNativeTable.rowType().getFields();
        ArrayList<Column> fullSchema = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true, field.description());
            fullSchema.add(column);
        }
        String comment = paimonNativeTable.comment().orElse("");
        PaimonTable table = new PaimonTable(this.catalogName, dbName, tblName, fullSchema, paimonNativeTable);
        table.setComment(comment);
        return table;
    }

    public Table getView(String dbName, String viewName) {
        org.apache.paimon.view.View paimonNativeView;
        try {
            paimonNativeView = paimonNativeCatalog.getView(new Identifier(dbName, viewName));
        } catch (Catalog.ViewNotExistException e) {
            LOG.error("Cannot find the paimon table or view {}.{}.", dbName, viewName, e);
            return null;
        }
        List<DataField> fields = paimonNativeView.rowType().getFields();
        List<Column> columns = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true, field.description());
            columns.add(column);
        }
        String comment = "";
        if (paimonNativeView.comment().isPresent()) {
            comment = paimonNativeView.comment().get();
        }
        String query;
        if (paimonNativeView.dialects().containsKey(VIEW_DIALECTS_KEY)) {
            query = paimonNativeView.dialects().get(VIEW_DIALECTS_KEY);
        } else {
            query = paimonNativeView.query();
        }
        PaimonView view = new PaimonView(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName, dbName, viewName,
                columns, query);
        view.setComment(comment);
        return view;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        try {
            this.paimonNativeCatalog.getTable(Identifier.create(dbName, tableName));
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to get Paimon table {}.{}.{}.", catalogName, dbName, tableName, e);
            return false;
        }
    }

    @Override
    public Map<String, Partition> getPartitions(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        RowType dataTableRowType;
        try {
            paimonTable = this.paimonNativeCatalog.getTable(identifier);
            dataTableRowType = paimonTable.rowType();
        } catch (Exception e) {
            LOG.error("Failed to get Paimon table {}.{}.{}.", catalogName, databaseName, tableName, e);
            throw new StarRocksConnectorException(e.getMessage());
        }
        List<String> partitionColumnNames = paimonTable.partitionKeys();
        if (partitionColumnNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Partition> partitionInfos = new HashMap<>();
        List<DataType> partitionColumnTypes = new ArrayList<>();
        for (String partitionColumnName : partitionColumnNames) {
            partitionColumnTypes.add(dataTableRowType.getTypeAt(dataTableRowType.getFieldIndex(partitionColumnName)));
        }

        try {
            List<org.apache.paimon.partition.Partition> partitions = paimonNativeCatalog.listPartitions(identifier);
            for (org.apache.paimon.partition.Partition partition : partitions) {
                // partition.spec() is a Map without a guaranteed iteration order. Build the value
                // list by looking up each partition column in declaration order so that values are
                // aligned with `partitionColumnNames` (which is what downstream code assumes).
                Map<String, String> spec = partition.spec();
                List<String> partitionValues = new ArrayList<>(partitionColumnNames.size());
                for (String partitionColumnName : partitionColumnNames) {
                    partitionValues.add(spec.get(partitionColumnName));
                }
                Partition srPartition = buildSrPartition(
                        partition.recordCount(), partition.fileSizeInBytes(), partition.fileCount(),
                        partitionColumnNames, partitionColumnTypes, partitionValues, partition.lastFileCreationTime());
                partitionInfos.put(srPartition.getPartitionName(), srPartition);
            }
            return partitionInfos;
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Failed to get partition info of paimon table {}.{}.", databaseName, tableName, e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private Partition buildSrPartition(Long recordCount,
                                       Long fileSizeInBytes,
                                       Long fileCount,
                                       List<String> partitionColumnNames,
                                       List<DataType> partitionColumnTypes,
                                       List<String> partitionValues,
                                       long lastUpdateTime) {
        if (partitionValues.size() != partitionColumnNames.size()) {
            throw new IllegalArgumentException(
                    String.format("The length of partitionValues %s is not equal to the partitionColumnNames %s.",
                            partitionValues.size(), partitionColumnNames.size()));
        }
        PaimonPartitionKey partitionKey = new PaimonPartitionKey();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionValues.size(); i++) {
            String column = partitionColumnNames.get(i);
            String value = partitionValues.get(i).trim();
            if (partitionColumnTypes.get(i) instanceof DateType) {
                if (!partitionKey.nullPartitionValueList().contains(value) && StringUtils.isNumeric(value)) {
                    value = DateTimeUtils.formatDate(Integer.parseInt(value));
                }
            }
            sb.append(column).append("=").append(value).append("/");
        }
        sb.deleteCharAt(sb.length() - 1);
        String partitionName = sb.toString();
        return new Partition(partitionName, lastUpdateTime, recordCount, fileSizeInBytes, fileCount);
    }

    @Override
    public long getTableUpdateTime(PaimonTable table) {
        Identifier identifier = new Identifier(table.getCatalogDBName(), table.getCatalogTableName());
        long lastCommitTime = -1;
        try {
            Optional<Snapshot> snapshotOptional = paimonNativeCatalog.getTable(identifier).latestSnapshot();
            if (snapshotOptional.isPresent()) {
                lastCommitTime = snapshotOptional.get().timeMillis();
            }
        } catch (Exception e) {
            LOG.error("Failed to get commit_time of paimon table {}.{}.",
                    table.getCatalogDBName(), table.getCatalogTableName(), e);
        }
        if (lastCommitTime == -1) {
            lastCommitTime = System.currentTimeMillis();
        }
        return lastCommitTime;
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames) {
        String tableName = table.getName();
        Identifier identifier = new Identifier(srDbName, tableName);
        paimonNativeCatalog.invalidateTable(identifier);
        try {
            ((PaimonTable) table).setPaimonNativeTable(paimonNativeCatalog.getTable(identifier));
            if (partitionNames != null && !partitionNames.isEmpty()) {
                // todo: paimon does not support to refresh an exact partition
                this.refreshPartitionInfo(identifier);
            } else {
                this.refreshPartitionInfo(identifier);
            }
            // Preheat manifest files, disabled by default
            if (Config.enable_paimon_refresh_manifest_files) {
                if (partitionNames == null || partitionNames.isEmpty()) {
                    ((PaimonTable) table).getNativeTable().newReadBuilder().newScan().plan();
                } else {
                    List<String> partitionColumnNames = table.getPartitionColumnNames();
                    Map<String, String> partitionSpec = new HashMap<>();
                    for (String partitionName : partitionNames) {
                        partitionSpec.put(String.join(",", partitionColumnNames), partitionName);
                    }
                    ((PaimonTable) table).getNativeTable().newReadBuilder()
                            .withPartitionFilter(partitionSpec).newScan().plan();
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to refresh table {}.{}.{}.", catalogName, srDbName, tableName, e);
        }
    }

    private void refreshPartitionInfo(Identifier identifier) {
        if (paimonNativeCatalog instanceof CachingCatalog) {
            try {
                paimonNativeCatalog.invalidateTable(identifier);
                ((CachingCatalog) paimonNativeCatalog).refreshPartitions(identifier);
            } catch (Exception e) {
                LOG.error("Failed to refresh paimon partitions {}.{}.", catalogName, identifier.getFullName(), e);
                throw new StarRocksConnectorException(e.getMessage(), e);
            }
        } else {
            LOG.warn("Current catalog {} does not support cache.", catalogName);
        }
    }

    @Override
    public void traceCatalogMetrics(String prefix, PaimonMetricRegistry metricRegistry) {
        // Don't need scan metrics when selecting system table, in which metric group is null.
        if (metricRegistry.getMetricGroup() == null) {
            return;
        }

        if (paimonNativeCatalog instanceof CachingCatalog) {
            CachingCatalog.CacheSizes cacheSizes = ((CachingCatalog) paimonNativeCatalog).estimatedCacheSizes();
            Tracers.record(EXTERNAL, prefix + "." + "catalogCache" + "." + "cachedDatabaseNumInCatalog",
                    String.valueOf(cacheSizes.databaseCacheSize()));
            Tracers.record(EXTERNAL, prefix + "." + "catalogCache" + "." + "cachedTableNumInCatalog",
                    String.valueOf(cacheSizes.tableCacheSize()));
            Tracers.record(EXTERNAL, prefix + "." + "catalogCache" + "." + "cachedManifestNumInCatalog",
                    String.valueOf(cacheSizes.manifestCacheSize()));
            Tracers.record(EXTERNAL, prefix + "." + "catalogCache" + "." + "cachedManifestBytesInCatalog",
                    cacheSizes.manifestCacheBytes() + " B");
            Tracers.record(EXTERNAL, prefix + "." + "catalogCache" + "." + "cachedPartitionNumInCatalog",
                    String.valueOf(cacheSizes.partitionCacheSize()));
        }
    }

    @Override
    public List<Split> getSplits(Table table, Map<String, Partition> partitions,
                                 Integer totalPartitionCount, long snapshotId, InnerTableScan scan) {
        return scan.plan().splits();
    }
}


