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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Schema;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplitAssigner;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OdpsMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(OdpsMetadata.class);
    public static final long NEVER_CACHE = 0;
    private final Odps odps;
    private final String catalogName;
    private final EnvironmentSettings settings;
    private final AliyunCloudCredential aliyunCloudCredential;
    private final OdpsProperties properties;
    private final ExecutorService pullRemoteFileExecutor;
    private final boolean schemaMode;
    private final String boundProject;

    private String catalogOwner;
    private LoadingCache<String, Set<String>> tableNameCache;
    private LoadingCache<OdpsTableName, OdpsTable> tableCache;
    private LoadingCache<OdpsTableName, List<PartitionSpec>> partitionCache;

    private static final long SMALL_LIMIT_THRESHOLD = 10000L;

    public OdpsMetadata(Odps odps, String catalogName, AliyunCloudCredential aliyunCloudCredential,
                        OdpsProperties properties) {
        this(odps, catalogName, aliyunCloudCredential, properties, null);
    }

    public OdpsMetadata(Odps odps, String catalogName, AliyunCloudCredential aliyunCloudCredential,
                        OdpsProperties properties, ExecutorService pullRemoteFileExecutor) {
        this.odps = odps;
        this.catalogName = catalogName;
        this.aliyunCloudCredential = aliyunCloudCredential;
        this.properties = properties;
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.schemaMode = Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_NAMESPACE_SCHEMA));
        this.boundProject = properties.get(OdpsProperties.PROJECT);
        EnvironmentSettings.Builder settingsBuilder =
                EnvironmentSettings.newBuilder().withServiceEndpoint(odps.getEndpoint())
                        .withCredentials(Credentials.newBuilder().withAccount(odps.getAccount()).build())
                        .withRestOptions(RestOptions.newBuilder()
                                .witUserAgent("StarRocks")
                                .build());
        if (!StringUtils.isNullOrEmpty(properties.get(OdpsProperties.TUNNEL_ENDPOINT))) {
            settingsBuilder.withTunnelEndpoint(properties.get(OdpsProperties.TUNNEL_ENDPOINT));
        }
        if (!StringUtils.isNullOrEmpty(properties.get(OdpsProperties.TUNNEL_QUOTA))) {
            settingsBuilder.withQuotaName(properties.get(OdpsProperties.TUNNEL_QUOTA));
        }
        settings = settingsBuilder.build();
        initMetaCache();
    }

    private void initMetaCache() {
        Executor executor = MoreExecutors.newDirectExecutorService();
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_TABLE_NAME_CACHE))) {
            tableNameCache =
                    newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.TABLE_NAME_CACHE_EXPIRE_TIME)),
                            Long.parseLong(properties.get(OdpsProperties.PROJECT_CACHE_SIZE)))
                            .build(asyncReloading(CacheLoader.from(this::loadTables), executor));
        } else {
            tableNameCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(CacheLoader.from(this::loadTables));
        }
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_TABLE_CACHE))) {
            tableCache = newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.TABLE_CACHE_EXPIRE_TIME)),
                    Long.parseLong(properties.get(OdpsProperties.TABLE_CACHE_SIZE)))
                    .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
        } else {
            tableCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
        }
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_PARTITION_CACHE))) {
            partitionCache = newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.PARTITION_CACHE_EXPIRE_TIME)),
                    Long.parseLong(properties.get(OdpsProperties.PARTITION_CACHE_SIZE)))
                    .build(asyncReloading(CacheLoader.from(this::loadPartitions), executor));
        } else {
            partitionCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadPartitions), executor));
        }
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.ODPS;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        if (schemaMode) {
            return listSchemaNames();
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try {
            if (StringUtils.isNullOrEmpty(catalogOwner)) {
                SecurityManager sm = odps.projects().get().getSecurityManager();
                String result = sm.runQuery("whoami", false);
                JsonObject js = JsonParser.parseString(result).getAsJsonObject();
                catalogOwner = js.get("DisplayName").getAsString();
            }
            Iterator<Project> iterator = odps.projects().iterator(catalogOwner);
            while (iterator.hasNext()) {
                Project project = iterator.next();
                builder.add(project.getName());
            }
        } catch (OdpsException e) {
            e.printStackTrace();
            throw new StarRocksConnectorException("fail to list project names", e);
        }
        ImmutableList<String> databases = builder.build();
        if (databases.isEmpty()) {
            return ImmutableList.of(odps.getDefaultProject());
        }
        return databases;
    }

    private List<String> listSchemaNames() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try {
            Iterator<Schema> iterator = odps.schemas().iterator(boundProject);
            while (iterator.hasNext()) {
                builder.add(iterator.next().getName());
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    "fail to list schemas of project " + boundProject + ": " + e.getMessage(), e);
        }
        return builder.build();
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        try {
            return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), name);
        } catch (StarRocksConnectorException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try {
            return new ArrayList<>(tableNameCache.get(dbName));
        } catch (ExecutionException e) {
            LOG.error("listTableNames error", e);
            return Collections.emptyList();
        }
    }

    private Set<String> loadTables(String dbName) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        Iterator<com.aliyun.odps.Table> iterator = schemaMode
                ? odps.tables().iterator(boundProject, dbName, null, false)
                : odps.tables().iterator(dbName);
        while (iterator.hasNext()) {
            builder.add(iterator.next().getName());
        }
        return builder.build();
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return get(tableCache, OdpsTableName.of(dbName, tblName));
    }

    private OdpsTable loadTable(OdpsTableName odpsTableName) {
        String dbName = odpsTableName.getDatabaseName();
        String tblName = odpsTableName.getTableName();
        com.aliyun.odps.Table table = schemaMode
                ? odps.tables().get(boundProject, dbName, tblName)
                : odps.tables().get(dbName, tblName);
        try {
            table.reload();
        } catch (OdpsException e) {
            return null;
        }
        if (schemaMode) {
            return new OdpsTable(catalogName, boundProject, dbName, table);
        }
        return new OdpsTable(catalogName, table);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        OdpsTableName odpsTableName = OdpsTableName.of(databaseName, tableName);
        // TODO: perhaps not good to support users to fetch whole tables?
        List<PartitionSpec> partitions = get(partitionCache, odpsTableName);
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }
        return partitions.stream().map(p -> p.toString(false, true)).collect(
                Collectors.toList());
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        List<PartitionSpec> partitionSpecs = get(partitionCache, OdpsTableName.of(databaseName, tableName));
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (partitionSpecs == null || partitionSpecs.isEmpty()) {
            return builder.build();
        }
        List<String> keys = new ArrayList<>(partitionSpecs.get(0).keys());
        for (PartitionSpec partitionSpec : partitionSpecs) {
            boolean present = true;
            for (int index = 0; index < keys.size(); index++) {
                String value = keys.get(index);
                if (partitionValues.get(index).isPresent() && partitionSpec.get(value) != null) {
                    if (!partitionSpec.get(value).equals(partitionValues.get(index).get())) {
                        present = false;
                        break;
                    }
                }
            }
            if (present) {
                builder.add(partitionSpec.toString(false, true));
            }
        }
        return builder.build();
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TableVersionRange version) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        // cause we don't know the real schema in file，just use the default Row Count now
        builder.setOutputRowCount(1);
        return builder.build();
    }

    private List<PartitionSpec> loadPartitions(OdpsTableName odpsTableName) {
        String dbName = odpsTableName.getDatabaseName();
        String tblName = odpsTableName.getTableName();
        com.aliyun.odps.Table odpsTable = schemaMode
                ? odps.tables().get(boundProject, dbName, tblName)
                : odps.tables().get(dbName, tblName);
        try {
            return odpsTable.getPartitionSpecs();
        } catch (OdpsException e) {
            String errorMsg =
                    "Encounter error when loading partitions of" + odpsTableName + ", error: " + e.getMessage() +
                            ", MaxCompute requestId " + e.getRequestId();
            LOG.error(errorMsg, e);
            throw new StarRocksConnectorException(errorMsg, e);
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return Collections.emptyList();
        }
        OdpsTable odpsTable = (OdpsTable) table;
        List<PartitionSpec> partitions = get(partitionCache,
                OdpsTableName.of(odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName()));
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> filter = new HashSet<>(partitionNames);
        return partitions.stream()
                .filter(partition -> filter.contains(partition.toString(false, true)))
                .map(p -> new OdpsPartition(getOdpsSdkTable(odpsTable), p))
                .collect(Collectors.toList());
    }

    private com.aliyun.odps.Table getOdpsSdkTable(OdpsTable odpsTable) {
        if (odpsTable.getSchemaName() != null) {
            return odps.tables().get(odpsTable.getProjectName(),
                    odpsTable.getSchemaName(), odpsTable.getCatalogTableName());
        }
        return odps.tables().get(odpsTable.getProjectName() != null
                        ? odpsTable.getProjectName() : odpsTable.getCatalogDBName(),
                odpsTable.getCatalogTableName());
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        OdpsTableName odpsTableName = OdpsTableName.of(srDbName, table.getName());
        tableCache.invalidate(odpsTableName);
        get(tableCache, odpsTableName);
        if (!table.isUnPartitioned()) {
            partitionCache.invalidate(odpsTableName);
            get(partitionCache, odpsTableName);
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        // add scanBuilder param for mock
        return getRemoteFiles(table, params, new TableReadSessionBuilder());
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        if (pullRemoteFileExecutor == null) {
            List<RemoteFileInfo> fileInfos = getRemoteFiles(table, params);
            return new RemoteFileInfoDefaultSource(fileInfos);
        }

        OdpsAsyncRemoteInfoSource remoteInfoSource = new OdpsAsyncRemoteInfoSource(
                pullRemoteFileExecutor, table, params, this);
        remoteInfoSource.run();
        return remoteInfoSource;
    }

    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params,
                                               TableReadSessionBuilder scanBuilder) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        OdpsTable odpsTable = (OdpsTable) table;
        Set<String> set = new HashSet<>(params.getFieldNames());
        List<String> orderedColumnNames = new ArrayList<>();
        for (Column column : odpsTable.getFullSchema()) {
            if (set.contains(column.getName())) {
                orderedColumnNames.add(column.getName());
            }
        }
        List<PartitionSpec> partitionSpecs = new ArrayList<>();
        if (params.getPartitionKeys() != null) {
            for (PartitionKey partitionKey : params.getPartitionKeys()) {
                String hivePartitionName = toHivePartitionName(odpsTable.getPartitionColumnNames(), partitionKey);
                if (!hivePartitionName.isEmpty()) {
                    partitionSpecs.add(new PartitionSpec(hivePartitionName));
                }
            }
        }
        try {
            LOG.info("get remote file infos, project:{}, schema:{}, table:{}, columns:{}",
                    odpsTable.getProjectName(), odpsTable.getSchemaName(),
                    odpsTable.getCatalogTableName(), params.getFieldNames());
            TableIdentifier identifier = odpsTable.getSchemaName() != null
                    ? TableIdentifier.of(odpsTable.getProjectName(),
                            odpsTable.getSchemaName(), odpsTable.getCatalogTableName())
                    : TableIdentifier.of(
                            odpsTable.getProjectName() != null
                                    ? odpsTable.getProjectName() : odpsTable.getCatalogDBName(),
                            odpsTable.getCatalogTableName());
            TableReadSessionBuilder tableReadSessionBuilder = scanBuilder.identifier(identifier)
                    .withSettings(settings).requiredDataColumns(orderedColumnNames).requiredPartitions(partitionSpecs);

            OdpsSplitsInfo odpsSplitsInfo;
            long limit = params.getLimit();

            if (limit > -1 && limit <= SMALL_LIMIT_THRESHOLD) {
                // Strategy 1: For small limit queries, use a single split based on row count.
                LOG.info("Small limit ({}) detected. Using single row-offset split strategy.", limit);
                tableReadSessionBuilder.withSplitOptions(SplitOptions.newBuilder().SplitByRowOffset().build());
                TableBatchReadSession session = buildSessionWithPredicate(tableReadSessionBuilder, params, odpsTable);
                odpsSplitsInfo = callRowOffsetSplitsInfo(session, limit);
            } else {
                // Strategy 2: For all other cases (full scan or large limit), use dynamic size-based splitting.
                long dynamicSplitSize = calculateDynamicSplitSize(
                        odpsTable.getDataColumnNames().size(),
                        orderedColumnNames.size());
                LOG.info("Using dynamic size-based split strategy. Calculated split size: {} bytes.", dynamicSplitSize);

                tableReadSessionBuilder.withSplitOptions(
                        SplitOptions.newBuilder().SplitByByteSize(dynamicSplitSize).build());

                TableBatchReadSession session = buildSessionWithPredicate(tableReadSessionBuilder, params, odpsTable);
                odpsSplitsInfo = callSizeSplitsInfo(session);
            }
            OdpsRemoteFileDesc odpsRemoteFileDesc = OdpsRemoteFileDesc.createOdpsRemoteFileDesc(odpsSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(odpsRemoteFileDesc);
            remoteFileInfo.setFiles(remoteFileDescs);
            return Lists.newArrayList(remoteFileInfo);
        } catch (Exception e) {
            LOG.error("getRemoteFileInfos error", e);
            throw new StarRocksConnectorException(
                    "Encounter error when try to split the maxcompute table: " + e.getMessage(), e);
        }
    }

    private TableBatchReadSession buildSessionWithPredicate(TableReadSessionBuilder builder,
                                                            GetRemoteFilesParams params,
                                                            OdpsTable odpsTable) throws IOException {
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_PREDICATE_PUSHDOWN))) {
            Predicate odpsPredicate = EntityConvertUtils.convertPredicate(params.getPredicate(),
                    ImmutableSet.copyOf(odpsTable.getPartitionColumnNames()));
            LOG.info("Try to push down predicate {}", odpsPredicate);
            builder.withFilterPredicate(odpsPredicate);
            try {
                return builder.buildBatchReadSession();
            } catch (IOException e) {
                LOG.warn("Push down predicate failed: {}. Falling back to scanning without predicate. Reason: {}",
                        odpsPredicate, e.getMessage());
                // fallback: 清除谓词并重试
                return builder.withFilterPredicate(Predicate.NO_PREDICATE).buildBatchReadSession();
            }
        } else {
            return builder.buildBatchReadSession();
        }
    }

    private long calculateDynamicSplitSize(int totalColumns, int selectedColumns) {
        long baseSplitSize = Long.parseLong(properties.get(OdpsProperties.SPLIT_SIZE_LIMIT));
        if (selectedColumns <= 0 || totalColumns <= 0) {
            return baseSplitSize;
        }
        double columnRatio = (double) totalColumns / selectedColumns;
        long dynamicSize = (long) (baseSplitSize * columnRatio);
        return Math.min(dynamicSize, baseSplitSize * 15);
    }

    private OdpsSplitsInfo callSizeSplitsInfo(TableBatchReadSession odpsTableScanSession) throws IOException {
        Map<String, String> splitProperties = getCommonSplitProperties();
        OdpsSplitsInfo odpsSplitsInfo;
        InputSplitAssigner assigner = odpsTableScanSession.getInputSplitAssigner();
        odpsSplitsInfo = new OdpsSplitsInfo(Arrays.asList(assigner.getAllSplits()), odpsTableScanSession,
                OdpsSplitsInfo.SplitPolicy.SIZE, splitProperties);
        return odpsSplitsInfo;
    }

    private OdpsSplitsInfo callRowOffsetSplitsInfo(TableBatchReadSession session, long limit)
            throws IOException {
        Map<String, String> splitProperties = getCommonSplitProperties();
        RowRangeInputSplitAssigner assigner = (RowRangeInputSplitAssigner) session.getInputSplitAssigner();
        long totalRowCount = assigner.getTotalRowCount();
        long rowsToRead = Math.min(limit, totalRowCount);
        if (rowsToRead <= 0) {
            return new OdpsSplitsInfo(Collections.emptyList(), session, OdpsSplitsInfo.SplitPolicy.ROW_OFFSET,
                    splitProperties);
        }
        InputSplit split = assigner.getSplitByRowOffset(0, rowsToRead);
        return new OdpsSplitsInfo(Collections.singletonList(split), session, OdpsSplitsInfo.SplitPolicy.ROW_OFFSET,
                splitProperties);
    }

    private Map<String, String> getCommonSplitProperties() {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("tunnel_endpoint", properties.get(OdpsProperties.TUNNEL_ENDPOINT));
        splitProperties.put("quota_name", properties.get(OdpsProperties.TUNNEL_QUOTA));
        return splitProperties;
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        AliyunCloudConfiguration configuration = new AliyunCloudConfiguration(aliyunCloudCredential);
        configuration.loadCommonFields(new HashMap<>(0));
        return configuration;
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.get(key);
        } catch (Exception e) {
            return null;
        }
    }
}
