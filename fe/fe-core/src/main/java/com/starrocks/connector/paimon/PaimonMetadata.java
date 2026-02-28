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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TPaimonCommitMessage;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.io.ByteArrayInputStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);

    public static final String PAIMON_PARTITION_NULL_VALUE = "null";
    private final PaimonCatalog paimonCatalog;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogName;
    private final Map<Identifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PredicateSearchKey, PaimonSplitsInfo> paimonSplits = new ConcurrentHashMap<>();
    private final ConnectorProperties properties;
    private final Map<Identifier, Map<String, Partition>> partitionInfos = new ConcurrentHashMap<>();

    public PaimonMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, PaimonCatalog paimonCatalog,
                          ConnectorProperties properties) {
        this.paimonCatalog = paimonCatalog;
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.properties = properties;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.PAIMON;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return paimonCatalog.listDbNames();
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException {
        paimonCatalog.createDb(dbName, properties);
    }

    @Override
    public void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws DdlException {
        paimonCatalog.dropDb(dbName, isForceDrop);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return paimonCatalog.listTableNames(dbName);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return paimonCatalog.createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        paimonCatalog.dropTable(stmt);
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        paimonCatalog.dropPartition(db, table, clause);
    }

    private Map<String, Partition> getOrLoadPartitionInfo(Identifier identifier) {
        return partitionInfos.computeIfAbsent(identifier, id -> {
            Map<String, Partition> map = this.paimonCatalog.getPartitions(id.getDatabaseName(), id.getTableName());
            return new ConcurrentHashMap<>(map);
        });
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        Identifier identifier = new Identifier(databaseName, tableName);
        Map<String, Partition> partitionInfo = getOrLoadPartitionInfo(identifier);
        return new ArrayList<>(partitionInfo.keySet());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        Database db = paimonCatalog.getDb(dbName);
        databases.put(dbName, db);
        return db;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }
        Table table = this.paimonCatalog.getTable(dbName, tblName);
        tables.put(identifier, table);
        return table;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tableName) {
        return this.paimonCatalog.tableExists(dbName, tableName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        PaimonTable paimonTable = (PaimonTable) table;
        Identifier identifier = new Identifier(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        long snapshotId = resolveSnapshotId(paimonTable);
        PredicateSearchKey filter = PredicateSearchKey.of(paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), snapshotId, params.getPredicate());
        if (!paimonSplits.containsKey(filter)) {
            ReadBuilder readBuilder = paimonTable.getNativeTable().newReadBuilder();
            int[] projected =
                    params.getFieldNames().stream().mapToInt(name -> (paimonTable.getFieldNames().indexOf(name))).toArray();
            List<ScalarOperator> originalConjuncts = Utils.extractConjuncts(params.getPredicate());
            List<Predicate> pushedPredicates = convertPredicates(paimonTable, originalConjuncts);
            readBuilder = readBuilder.withFilter(pushedPredicates);
            boolean countOptimize = params.getFieldNames().size() == 1 &&
                    params.getFieldNames().get(0).equalsIgnoreCase("___COUNT___");
            if (!countOptimize) {
                readBuilder = readBuilder.withProjection(projected);
            }
            boolean pruneManifestsByLimit = params.getLimit() != -1 && params.getLimit() < Integer.MAX_VALUE
                     && onlyHasPartitionPredicate(table, originalConjuncts)
                    && originalConjuncts.size() == pushedPredicates.size();
            if (pruneManifestsByLimit) {
                readBuilder = readBuilder.withLimit((int) params.getLimit());
            }
            InnerTableScan scan = (InnerTableScan) readBuilder.newScan();
            PaimonMetricRegistry paimonMetricRegistry = new PaimonMetricRegistry();
            scan.withMetricRegistry(paimonMetricRegistry);
            Map<String, Partition> partitions = new HashMap<>();
            Integer totalPartitionCount = 1;
            // Only use partition pruning for READY tables
            if (paimonTable.getLakeOptimizerMode() == PaimonTable.LakeOptimizerMode.READY) {
                Map<String, Partition> cachedPartitionInfo = getOrLoadPartitionInfo(identifier);
                for (PartitionKey partitionKey : params.getPartitionKeys()) {
                    String name = partitionKey.getName();
                    partitions.put(name, cachedPartitionInfo.get(name));
                }
                totalPartitionCount = cachedPartitionInfo.size();
            }

            List<Split> splits = paimonCatalog.getSplits(table, partitions,
                    totalPartitionCount, snapshotId, scan);
            this.traceScanMetrics(paimonMetricRegistry, splits, table.getCatalogTableName(),
                    pushedPredicates, String.valueOf(Objects.hash(params.getPredicate())));

            PaimonSplitsInfo paimonSplitsInfo = new PaimonSplitsInfo(pushedPredicates, splits);
            paimonSplits.put(filter, paimonSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPaimonRemoteFileDesc(paimonSplitsInfo));
            remoteFileInfo.setFiles(remoteFileDescs);
        } else {
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    PaimonRemoteFileDesc.createPaimonRemoteFileDesc(paimonSplits.get(filter)));
            remoteFileInfo.setFiles(remoteFileDescs);
        }

        return Lists.newArrayList(remoteFileInfo);
    }

    private long resolveSnapshotId(PaimonTable paimonTable) {
        if (paimonTable.getLakeOptimizerMode() == PaimonTable.LakeOptimizerMode.READY) {
            return paimonTable.getEndSnapshot();
        }
        try {
            if (paimonTable.getNativeTable().latestSnapshot().isPresent()) {
                return paimonTable.getNativeTable().latestSnapshot().get().id();
            }
        } catch (Exception e) {
            LOG.warn("Cannot get snapshot because {}", e.getMessage());
        }
        return -1L;
    }

    private void traceScanMetrics(PaimonMetricRegistry metricRegistry,
                                  List<Split> splits,
                                  String tableName,
                                  List<Predicate> predicates,
                                  String predicateHash) {
        // Don't need scan metrics when selecting system table, in which metric group is null.
        if (metricRegistry.getMetricGroup() == null) {
            return;
        }
        String prefix = "Paimon.plan." + tableName + "-" + predicateHash;

        for (int i = 0; i < predicates.size(); i++) {
            Tracers.record(EXTERNAL, prefix + ".filter." + i, predicates.get(i).toString());
        }

        Map<String, Metric> metrics = metricRegistry.getMetrics();
        long scanDuration = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_DURATION)).getValue();
        long scanSnapshotId = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCANNED_SNAPSHOT_ID)).getValue();
        long scannedManifests = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCANNED_MANIFESTS)).getValue();
        long skippedTableFiles = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES)).getValue();
        long resultedTableFiles = (long) ((Gauge<?>) metrics.get(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES)).getValue();
        long manifestNumReadFromCache = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_HIT_CACHE)).getValue();
        long manifestNumReadFromRemote = (long) ((Gauge<?>) metrics.get(ScanMetrics.MANIFEST_MISSED_CACHE)).getValue();
        long dvMetaNumReadFromCache = (long) ((Gauge<?>) metrics.get(ScanMetrics.DVMETA_HIT_CACHE)).getValue();
        long dvMetaNumReadFromRemote = (long) ((Gauge<?>) metrics.get(ScanMetrics.DVMETA_MISSED_CACHE)).getValue();

        Tracers.record(EXTERNAL, prefix + "." + "planTime", scanDuration + "ms");
        Tracers.record(EXTERNAL, prefix + "." + "snapshotId", String.valueOf(scanSnapshotId));
        Tracers.record(EXTERNAL, prefix + "." + "scannedManifestsNum", String.valueOf(scannedManifests));
        Tracers.record(EXTERNAL, prefix + "." + "skippedDataFilesNum", String.valueOf(skippedTableFiles));
        Tracers.record(EXTERNAL, prefix + "." + "resultedDataFilesNum", String.valueOf(resultedTableFiles));
        Tracers.record(EXTERNAL, prefix + "." + "objectsCache" + "." + "numReadFromCache",
                String.valueOf(manifestNumReadFromCache));
        Tracers.record(EXTERNAL, prefix + "." + "objectsCache" + "." + "numReadFromRemote",
                String.valueOf(manifestNumReadFromRemote));
        Tracers.record(EXTERNAL, prefix + "." + "dvMetaCache" + "." + "numReadFromCache",
                String.valueOf(dvMetaNumReadFromCache));
        Tracers.record(EXTERNAL, prefix + "." + "dvMetaCache" + "." + "numReadFromRemote",
                String.valueOf(dvMetaNumReadFromRemote));
        Tracers.record(EXTERNAL, prefix + "." + "splitsNum", String.valueOf(splits.size()));

        AtomicLong resultedTableFilesSize = new AtomicLong(0);
        for (Split split : splits) {
            List<DataFileMeta> dataFileMetas = ((DataSplit) split).dataFiles();
            dataFileMetas.forEach(dataFileMeta -> resultedTableFilesSize.addAndGet(dataFileMeta.fileSize()));
        }
        Tracers.record(EXTERNAL, prefix + "." + "resultedDataFilesSize", resultedTableFilesSize.get() + " B");
        paimonCatalog.traceCatalogMetrics(prefix, metricRegistry);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange versionRange) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "GetPaimonTableStatistics")) {
            if (!properties.enableGetTableStatsFromExternalMetadata()) {
                return StatisticsUtils.buildDefaultStatistics(columns.keySet());
            }

            Statistics.Builder builder = Statistics.builder();
            if (session.getSessionVariable().enablePaimonColumnStatistics()) {
                org.apache.paimon.table.Table nativeTable = ((PaimonTable) table).getNativeTable();
                Optional<org.apache.paimon.stats.Statistics> statistics = nativeTable.statistics();
                if (!statistics.isPresent() || statistics.get().colStats() == null
                        || !statistics.get().mergedRecordCount().isPresent()) {
                    return defaultStatistics(session, columns, table, predicate, limit);
                }
                long rowCount = statistics.get().mergedRecordCount().getAsLong();
                builder.setOutputRowCount(rowCount);
                Map<String, ColStats<?>> colStatsMap = statistics.get().colStats();
                for (ColumnRefOperator column : columns.keySet()) {
                    builder.addColumnStatistic(column, buildColumnStatistic(columns.get(column), colStatsMap, rowCount));
                }
                return builder.build();
            } else {
                return defaultStatistics(session, columns, table, predicate, limit);
            }
        }
    }

    private Statistics defaultStatistics(OptimizerContext session, Map<ColumnRefOperator, Column> columns, Table table,
                                         ScalarOperator predicate, long limit) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        if (session.getSessionVariable().enablePaimonEstimatedStatistics()) {
            List<String> fieldNames = columns.keySet().stream().map(ColumnRefOperator::getName).collect(Collectors.toList());
            GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                    .setPredicate(predicate).setFieldNames(fieldNames).setLimit(limit).build();
            List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getRemoteFiles(table, params);
            PaimonRemoteFileDesc remoteFileDesc = (PaimonRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
            List<Split> splits = remoteFileDesc.getPaimonSplitsInfo().getPaimonSplits();
            long rowCount = getRowCount(splits);
            if (rowCount == 0) {
                builder.setOutputRowCount(1);
            } else {
                builder.setOutputRowCount(rowCount);
            }
        } else {
            builder.setOutputRowCount(1);
        }
        return builder.build();
    }


    private ColumnStatistic buildColumnStatistic(Column column, Map<String, ColStats<?>> colStatsMap,
                                                 long rowCount) {
        ColumnStatistic columnStatistic = null;
        for (Map.Entry<String, ColStats<?>> colStatsEntry : colStatsMap.entrySet()) {
            if (!colStatsEntry.getKey().equalsIgnoreCase(column.getName())) {
                continue;
            }
            ColumnStatistic.Builder builder = ColumnStatistic.builder();
            ColStats<?> colStats = colStatsEntry.getValue();
            Optional<? extends Comparable<?>> min = colStats.min();
            if (min.isPresent() && min.get() != null) {
                if (column.getType().isBoolean()) {
                    builder.setMinValue((Boolean) min.get() ? 1 : 0);
                } else if (column.getType().isDatetime()) {
                    builder.setMinValue(getLongFromDateTime(((Timestamp) min.get()).toLocalDateTime()));
                } else {
                    builder.setMinValue(Double.parseDouble(min.get().toString()));
                }
            }

            Optional<? extends Comparable<?>> max = colStats.max();
            if (max.isPresent() && max.get() != null) {
                if (column.getType().isBoolean()) {
                    builder.setMaxValue((Boolean) max.get() ? 1 : 0);
                } else if (column.getType().isDatetime()) {
                    builder.setMaxValue(getLongFromDateTime(((Timestamp) max.get()).toLocalDateTime()));
                } else if (!column.getType().isBoolean()) {
                    builder.setMaxValue(Double.parseDouble(max.get().toString()));
                }
            }

            if (colStats.nullCount().isPresent()) {
                builder.setNullsFraction(colStats.nullCount().getAsLong() * 1.0 / Math.max(rowCount, 1));
            } else {
                builder.setNullsFraction(0);
            }

            builder.setAverageRowSize(colStats.avgLen().isPresent() ? colStats.avgLen().getAsLong() : 1);

            if (colStats.distinctCount().isPresent()) {
                builder.setDistinctValuesCount(colStats.distinctCount().getAsLong());
                builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
            } else {
                builder.setDistinctValuesCount(1);
                builder.setType(ColumnStatistic.StatisticType.UNKNOWN);
            }
            columnStatistic = builder.build();
        }

        if (columnStatistic == null) {
            columnStatistic = ColumnStatistic.unknown();
        }
        return columnStatistic;
    }

    public static long getRowCount(List<? extends Split> splits) {
        long rowCount = 0;
        for (Split split : splits) {
            rowCount += split.rowCount();
        }
        return rowCount;
    }

    private List<Predicate> convertPredicates(PaimonTable paimonTable, List<ScalarOperator> originalConjuncts) {
        List<Predicate> predicates = new ArrayList<>(originalConjuncts.size());
        ZoneId sessionZoneId = ZoneId.of(TimeUtils.getSessionTimeZone());
        PaimonPredicateConverter converter =
                new PaimonPredicateConverter(paimonTable.getNativeTable().rowType(), sessionZoneId);
        for (ScalarOperator operator : originalConjuncts) {
            Predicate filter = converter.convert(operator);
            if (filter != null) {
                predicates.add(filter);
            }
        }
        return predicates;
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        Identifier identifier = new Identifier(paimonTable.getCatalogDBName(), paimonTable.getCatalogTableName());
        List<PartitionInfo> result = new ArrayList<>();
        if (table.isUnPartitioned()) {
            result.add(new Partition(paimonTable.getCatalogTableName(),
                    this.paimonCatalog.getTableUpdateTime(paimonTable), null,
                    null, null));
            return result;
        }
        Map<String, Partition> partitionInfo = getOrLoadPartitionInfo(identifier);
        for (String partitionName : partitionNames) {
            Partition partition = partitionInfo.get(partitionName);
            if (partition != null) {
                result.add(partition);
            } else {
                LOG.warn("Cannot find the paimon partition info: {}", partitionName);
            }
        }
        return result;
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        String tableName = table.getCatalogTableName();
        Identifier identifier = new Identifier(srDbName, tableName);
        paimonCatalog.refreshTable(srDbName, table, partitionNames);
        tables.put(identifier, table);
    }

    @Override
    public void finishSink(String dbName, String tblName, List<TSinkCommitInfo> commitInfos, String branch) {
        Identifier identifier = new Identifier(dbName, tblName);
        List<TPaimonCommitMessage> commitMessageList = commitInfos.stream()
                .map(TSinkCommitInfo::getPaimon_commit_message).collect(Collectors.toList());

        try {
            PaimonTable paimonTable = (PaimonTable) getTable(ConnectContext.get(), dbName, tblName);
            org.apache.paimon.table.Table paimonNativeTable = paimonTable.getNativeTable();
            BatchWriteBuilder builder = paimonNativeTable.newBatchWriteBuilder();

            if (commitInfos.get(0).isIs_overwrite()) {
                builder.withOverwrite(new HashMap<>());
            }
            BatchTableCommit commit = builder.newCommit();

            List<CommitMessage> messList = new ArrayList<>();
            CommitMessageSerializer commitMessageSerializer = new CommitMessageSerializer();

            for (TPaimonCommitMessage tPaimonCommitMessage : commitMessageList) {
                byte[] commitMessage = tPaimonCommitMessage.getCommit_message();
                if (tPaimonCommitMessage.isFrom_jni_writer()) {
                    commitMessage = Base64.getDecoder().decode(tPaimonCommitMessage.getCommit_message());
                }
                ByteArrayInputStream bis = new ByteArrayInputStream(commitMessage);
                List<CommitMessage> commitMessages = commitMessageSerializer.deserializeList(
                        tPaimonCommitMessage.getVersion(), new DataInputViewStreamWrapper(bis));
                messList.addAll(commitMessages);
            }
            commit.commit(messList);
            commit.close();
        } catch (Exception e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    public static boolean onlyHasPartitionPredicate(Table table, List<ScalarOperator> originalConjuncts) {
        if (originalConjuncts.isEmpty()) {
            return true;
        }
        List<String> partitionColNames = table.getPartitionColumnNames();
        for (ScalarOperator operator : originalConjuncts) {
            String columnName = null;
            if (operator.getChild(0) instanceof ColumnRefOperator) {
                columnName = ((ColumnRefOperator) operator.getChild(0)).getName();
            }
            if (columnName == null || columnName.isEmpty()) {
                return false;
            }
            if (!partitionColNames.contains(columnName)) {
                return false;
            }
        }
        return true;
    }
}
