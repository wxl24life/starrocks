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

package com.starrocks.lakeoptimizer.query;

import com.google.common.collect.Lists;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.lakeoptimizer.LakeOptimizerUtils;
import com.starrocks.lakeoptimizer.cache.TableSchemaInfo;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TMetadataEntry;
import com.starrocks.thrift.TPaimonFileMetadata;
import com.starrocks.thrift.TPaimonMetadataType;
import com.starrocks.thrift.TPaimonPartitionMetadata;
import com.starrocks.thrift.TPaimonSchemaMetadata;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntryWithDeletionFile;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for querying LakeOptimizer entity tables.
 * Handles SQL building, execution, and result deserialization.
 */
public class LakeOptimizerQueryService {
    private static final Logger LOG = LogManager.getLogger(LakeOptimizerQueryService.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";

    private final LakeOptimizerSqlBuilder sqlBuilder;

    public LakeOptimizerQueryService() {
        this.sqlBuilder = new LakeOptimizerSqlBuilder();
    }

    // ==================== Query Execution ====================

    /**
     * Execute a SQL query against LakeOptimizer entity tables.
     *
     * @param sql The SQL to execute
     * @param metadataType The type of metadata being queried (for result serialization)
     * @return List of result batches from the query
     */
    public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType metadataType) {
        ConnectContext prev = ConnectContext.get();
        // don't record internal sql statistics into profile, which will make profile nest too many levels
        Tracers.init(Tracers.Mode.NONE, Tracers.Module.NONE, false, false);

        try {
            ConnectContext context = LakeOptimizerUtils.buildConnectContext();
            context.setThreadLocalInfo();
            context.setQueryId(UUIDUtil.genUUID());

            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.METADATA_PAIMON);

            if (execPlan.getFragments().get(0).getSink() instanceof ResultSink) {
                ResultSink resultSink = (ResultSink) execPlan.getFragments().get(0).getSink();
                resultSink.setPaimonMetadataType(metadataType);
            }

            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);

            Pair<List<TResultBatch>, Status> result = executor.executeStmtWithExecPlan(context, execPlan);

            if (!result.second.ok()) {
                String errorMsg = String.format("Query failed | QueryId=%s | Error=%s | SQL=%s",
                        DebugUtil.printId(context.getQueryId()),
                        context.getState().getErrorMessage(),
                        sql);
                LOG.warn("{} {}", LOG_PREFIX, errorMsg);
                throw new RuntimeException(errorMsg);
            }

            AuditLog.getInternalAudit().info("{} Query | QueryId={} | SQL={}",
                    LOG_PREFIX, DebugUtil.printId(context.getQueryId()), sql);

            return result.first;
        } catch (Exception e) {
            LOG.error("{} Query execution failed | SQL={}", LOG_PREFIX, sql, e);
            throw new RuntimeException("Query execution failed: " + sql, e);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
                Tracers.init(prev, Tracers.Mode.NONE, null);
            }
        }
    }

    // ==================== Schema/Partition Query Methods ====================
    /**
     * Query table schema info by catalog/database/table name.
     */
    public TableSchemaInfo queryTableSchemaByName(String catalogName, String databaseName, String tableName) {
        String sql = sqlBuilder.buildSchemaQueryByName(catalogName, databaseName, tableName);
        List<TResultBatch> results = executeQuery(sql, TPaimonMetadataType.SCHEMA_METADATA);
        List<TableSchemaInfo> schemas = parseSchemaResults(results);
        return schemas.isEmpty() ? null : schemas.get(0);
    }

    /**
     * Query partitions for the given table and snapshot.
     */
    public List<Partition> queryPartitions(Long tableId, Long snapshotId) {
        String sql = sqlBuilder.buildPartitionsQuery(tableId, snapshotId);
        List<TResultBatch> results = executeQuery(sql, TPaimonMetadataType.PARTITION_METADATA);
        List<Partition> partitions = parsePartitionResults(results);
        LOG.debug("{} queryPartitions | tableId={} | snapshotId={} | count={}",
                LOG_PREFIX, tableId, snapshotId, partitions.size());
        return partitions;
    }

    // ==================== File Statistics Query Methods ====================

    /**
     * Query manifest entries with deletion files for a single partition and bucket.
     */
    public List<ManifestEntryWithDeletionFile> queryManifestSingleKey(long tableId, long snapshotId,
                                                       Partition partition, int bucket) {
        String partitionName = partition.getPartitionName();
        String sql = sqlBuilder.buildFileStatsQuery(tableId, partitionName, bucket, snapshotId);
        List<TResultBatch> resultBatch = executeQuery(sql, TPaimonMetadataType.FILE_METADATA);
        List<ManifestEntryWithDeletionFile> entries = parseFileStatsResults(
                resultBatch, Map.of(partitionName, partition), false);
        LOG.debug("{} queryManifestSingleKey | tableId={} | partition={} | bucket={} | count={}",
                LOG_PREFIX, tableId, partitionName, bucket, entries.size());
        return entries;
    }

    /**
     * Query manifest entries with deletion files using partition/bucket predicates.
     */
    public List<ManifestEntryWithDeletionFile> queryManifestFiltered(long tableId, long snapshotId,
                                                      Map<String, Partition> partitionMap,
                                                      Map<String, Set<Integer>> partitionBuckets,
                                                      boolean isUnpartitioned) {
        String sql = sqlBuilder.buildFileStatsQueryWithPredicates(tableId, snapshotId, partitionBuckets);
        List<TResultBatch> resultBatch = executeQuery(sql, TPaimonMetadataType.FILE_METADATA);
        List<ManifestEntryWithDeletionFile> entries = parseFileStatsResults(
                resultBatch, partitionMap, isUnpartitioned);
        LOG.debug("{} queryManifestFiltered | tableId={} | partitions={} | count={}",
                LOG_PREFIX, tableId, partitionBuckets.size(), entries.size());
        return entries;
    }

    /**
     * Query all manifest entries with deletion files for a table/snapshot without partition/bucket predicates.
     */
    public List<ManifestEntryWithDeletionFile> queryManifestAll(long tableId, long snapshotId,
                                                 Map<String, Partition> partitionMap,
                                                 boolean isUnpartitioned) {
        String sql = sqlBuilder.buildAllFileStatsQuery(tableId, snapshotId);
        List<TResultBatch> resultBatch = executeQuery(sql, TPaimonMetadataType.FILE_METADATA);
        List<ManifestEntryWithDeletionFile> entries = parseFileStatsResults(
                resultBatch, partitionMap, isUnpartitioned);
        LOG.debug("{} queryManifestAll | tableId={} | snapshotId={} | count={}",
                LOG_PREFIX, tableId, snapshotId, entries.size());
        return entries;
    }

    // ==================== Result Parsing ====================
    private List<TableSchemaInfo> parseSchemaResults(List<TResultBatch> resultBatches) {
        List<TableSchemaInfo> schemas = Lists.newArrayList();
        if (resultBatches == null || resultBatches.isEmpty()) {
            return schemas;
        }
        try {
            TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
            for (TResultBatch batch : resultBatches) {
                if (batch.rows == null) {
                    continue;
                }
                for (ByteBuffer rowBuffer : batch.rows) {
                    TMetadataEntry entry = deserializeMetadataEntry(deserializer, rowBuffer);
                    TableSchemaInfo schema = parseSchemaFromThrift(entry);
                    schemas.add(schema);
                }
            }
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to parse schema results", e);
        }
        return schemas;
    }

    private TableSchemaInfo parseSchemaFromThrift(TMetadataEntry entry) {
        if (!entry.isSetPaimon_schema_metadata()) {
            throw new RuntimeException("Paimon schema metadata not found");
        }
        TPaimonSchemaMetadata thrift = entry.getPaimon_schema_metadata();
        return new TableSchemaInfo(
                thrift.getTable_id(),
                thrift.getCatalog_name(),
                thrift.getDatabase_name(),
                thrift.getTable_name(),
                thrift.getTable_uuid(),
                thrift.getBegin_snapshot(),
                thrift.getEnd_snapshot(),
                thrift.getBucket_num()
        );
    }

    private List<Partition> parsePartitionResults(List<TResultBatch> resultBatches) {
        List<Partition> partitions = Lists.newArrayList();
        if (resultBatches == null || resultBatches.isEmpty()) {
            return partitions;
        }
        try {
            TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
            for (TResultBatch batch : resultBatches) {
                if (batch.rows == null) {
                    continue;
                }
                for (ByteBuffer rowBuffer : batch.rows) {
                    TMetadataEntry entry = deserializeMetadataEntry(deserializer, rowBuffer);
                    Partition partition = parsePartitionFromThrift(entry);
                    partitions.add(partition);
                }
            }
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to parse partition results", e);
        }
        return partitions;
    }

    private Partition parsePartitionFromThrift(TMetadataEntry entry) {
        if (!entry.isSetPaimon_partition_metadata()) {
            throw new RuntimeException("Paimon partition metadata not found");
        }
        TPaimonPartitionMetadata thrift = entry.getPaimon_partition_metadata();
        Partition partition = new Partition(
                thrift.getPartition_name(),
                thrift.getLast_file_creation_time(),
                thrift.getRow_count(),
                thrift.getData_size(),
                thrift.getFile_count()
        );
        partition.setPartitionValue(SerializationUtils.deserializeBinaryRow(thrift.getPartition_values()));
        return partition;
    }

    private List<ManifestEntryWithDeletionFile> parseFileStatsResults(List<TResultBatch> resultBatches,
                                                      Map<String, Partition> partitionMap,
                                                      boolean isUnpartitionedTable) {
        if (resultBatches == null || resultBatches.isEmpty()) {
            return Lists.newArrayList();
        }
        List<ByteBuffer> allRows = Lists.newArrayList();
        for (TResultBatch batch : resultBatches) {
            if (batch.rows != null) {
                allRows.addAll(batch.rows);
            }
        }
        if (allRows.isEmpty()) {
            return Lists.newArrayList();
        }

        return allRows.parallelStream()
                .map(rowBuffer -> {
                    try {
                        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
                        TMetadataEntry metadataEntry = deserializeMetadataEntry(deserializer, rowBuffer);
                        return parseManifestEntryWithDeletionFileFromThrift(
                                metadataEntry, partitionMap, isUnpartitionedTable);
                    } catch (Exception e) {
                        LOG.error("{} Failed to parse manifest entry with deletion file", LOG_PREFIX, e);
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private TMetadataEntry deserializeMetadataEntry(TDeserializer deserializer, ByteBuffer bb) {
        TMetadataEntry entry = new TMetadataEntry();
        byte[] bytes = new byte[bb.limit() - bb.position()];
        bb.get(bytes);
        try {
            deserializer.deserialize(entry, bytes);
        } catch (TException e) {
            LOG.error("{} Failed to deserialize TMetadataEntry", LOG_PREFIX, e);
            throw new RuntimeException(e);
        }
        return entry;
    }

    /**
     * Parse ManifestEntryWithDeletionFile from Thrift metadata entry.
     * Includes DeletionFile information if present.
     */
    private ManifestEntryWithDeletionFile parseManifestEntryWithDeletionFileFromThrift(TMetadataEntry entry,
                                                       Map<String, Partition> partitionMap,
                                                       boolean isUnpartitionedTable) {
        if (!entry.isSetPaimon_file_metadata()) {
            throw new RuntimeException("Paimon file metadata not found");
        }

        TPaimonFileMetadata thrift = entry.getPaimon_file_metadata();

        // Parse basic fields
        String fileName = thrift.getFile_name();
        long fileSize = thrift.getFile_size();
        long rowCount = thrift.getRow_count();
        int bucket = thrift.getBucket();
        long schemaId = thrift.getSchema_id();
        long creationTime = thrift.getCreation_time();
        int level = thrift.getLevel();
        long minSeqNum = thrift.getMin_sequence_number();
        long maxSeqNum = thrift.getMax_sequence_number();

        // Parse min/max key
        BinaryRow minKey = deserializeBinaryRowOrEmpty(thrift.isSetMin_key() ? thrift.getMin_key() : null);
        BinaryRow maxKey = deserializeBinaryRowOrEmpty(thrift.isSetMax_key() ? thrift.getMax_key() : null);

        // Parse stats
        SimpleStats keyStats = reconstructStats(
                thrift.isSetKey_stats_min() ? thrift.getKey_stats_min() : null,
                thrift.isSetKey_stats_max() ? thrift.getKey_stats_max() : null,
                thrift.isSetKey_stats_null_count() ? thrift.getKey_stats_null_count() : null);
        SimpleStats valueStats = reconstructStats(
                thrift.isSetValue_stats_min() ? thrift.getValue_stats_min() : null,
                thrift.isSetValue_stats_max() ? thrift.getValue_stats_max() : null,
                thrift.isSetValue_stats_null_count() ? thrift.getValue_stats_null_count() : null);

        // Parse extra_files
        List<String> extraFiles = thrift.isSetExtra_files() ? thrift.getExtra_files() : Collections.emptyList();

        // Parse NULLABLE fields
        Long deleteRowCount = thrift.isSetDelete_row_count() ? thrift.getDelete_row_count() : null;
        byte[] embeddedIndex = thrift.isSetEmbedded_file_index() ? thrift.getEmbedded_file_index() : null;
        FileSource fileSource = null;
        if (thrift.isSetFile_source()) {
            fileSource = FileSource.values()[thrift.getFile_source()];
        }
        List<String> valueStatsCols = thrift.isSetValue_stats_cols() ? thrift.getValue_stats_cols() : null;
        String externalPath = thrift.isSetExternal_path() ? thrift.getExternal_path() : null;
        Long firstRowId = thrift.isSetFirst_row_id() ? thrift.getFirst_row_id() : null;
        List<String> writeCols = thrift.isSetWrite_cols() ? thrift.getWrite_cols() : null;

        DataFileMeta fileMeta = DataFileMeta.create(
                fileName, fileSize, rowCount, minKey, maxKey, keyStats, valueStats,
                minSeqNum, maxSeqNum, schemaId, level, extraFiles,
                Timestamp.fromEpochMillis(creationTime),
                deleteRowCount, embeddedIndex, fileSource, valueStatsCols,
                externalPath, firstRowId, writeCols);

        // Resolve partition
        BinaryRow partition;
        if (isUnpartitionedTable) {
            partition = BinaryRow.EMPTY_ROW;
        } else {
            Partition p = partitionMap.get(thrift.getPartition_name());
            if (p == null) {
                throw new RuntimeException("Partition not found: " + thrift.getPartition_name());
            }
            partition = p.getPartitionValue();
        }

        int totalBuckets = thrift.getTotal_buckets();
        FileKind fileKind = FileKind.fromByteValue((byte) thrift.getFile_kind());
        ManifestEntry manifestEntry = ManifestEntry.create(fileKind, partition, bucket, totalBuckets, fileMeta);
        // Parse DeletionFile if present
        DeletionFile deletionFile = null;
        if (thrift.isSetDeletion_path()) {
            deletionFile = new DeletionFile(
                    thrift.getDeletion_path(),
                    thrift.getDeletion_offset(),
                    thrift.getDeletion_length(),
                    thrift.getDeletion_cardinality());
        }
        return new ManifestEntryWithDeletionFile(manifestEntry, deletionFile);
    }

    /**
     * Deserialize a BinaryRow from byte[], returning EMPTY_ROW for null/empty input.
     *
     * <p>Uses Paimon's {@link SerializationUtils#deserializeBinaryRow(byte[])} which reads the
     * schemaless format: [4-byte arity] + [binary content], then points the BinaryRow to the
     * underlying byte array at offset 4 .
     *
     * @see org.apache.paimon.io.DataFileMetaSerializer#fromRow — deserializes minKey/maxKey
     * @see org.apache.paimon.manifest.ManifestEntrySerializer#convertFrom — deserializes partition
     */
    private BinaryRow deserializeBinaryRowOrEmpty(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return BinaryRow.EMPTY_ROW;
        }
        return SerializationUtils.deserializeBinaryRow(bytes);
    }

    /**
     * Reconstruct SimpleStats from serialized byte arrays.
     *
     * <p>BinaryRow fields (minValues/maxValues) are deserialized via
     * {@link SerializationUtils#deserializeBinaryRow(byte[])}.
     *
     * <p>BinaryArray field (nullCounts) is restored by pointing to a MemorySegment wrapping the
     * raw bytes. This is equivalent to Paimon's native approach where nullCounts is stored as
     * ARRAY&lt;BIGINT&gt; — the underlying binary layout is identical.
     *
     * @see org.apache.paimon.stats.SimpleStats#fromRow — Paimon's native deserialization
     */
    private SimpleStats reconstructStats(byte[] minBytes, byte[] maxBytes, byte[] nullCountBytes) {
        BinaryRow minValues = null;
        BinaryRow maxValues = null;
        BinaryArray nullCounts = new BinaryArray();

        if (minBytes != null && minBytes.length > 0) {
            minValues = SerializationUtils.deserializeBinaryRow(minBytes);
        }
        if (maxBytes != null && maxBytes.length > 0) {
            maxValues = SerializationUtils.deserializeBinaryRow(maxBytes);
        }
        if (nullCountBytes != null && nullCountBytes.length > 0) {
            nullCounts.pointTo(MemorySegment.wrap(nullCountBytes), 0, nullCountBytes.length);
        }

        return new SimpleStats(minValues, maxValues, nullCounts);
    }
}
