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

import com.starrocks.lakeoptimizer.LakeOptimizerConstants;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL builder for LakeOptimizer entity table queries.
 */
public class LakeOptimizerSqlBuilder {

    private static final String FILE_STATS_COLUMNS =
            "partition_name, bucket, total_buckets, file_kind, " +
            "file_name, file_size, row_count, " +
            "min_key, max_key, " +
            "key_stats_min, key_stats_max, key_stats_null_count, " +
            "value_stats_min, value_stats_max, value_stats_null_count, " +
            "min_sequence_number, max_sequence_number, " +
            "schema_id, level, extra_files, creation_time, " +
            "delete_row_count, embedded_file_index, file_source, " +
            "value_stats_cols, external_path, first_row_id, write_cols, " +
            "deletion_path, deletion_offset, deletion_length, deletion_cardinality";

    /**
     * Build SQL to query file statistics for a specific partition and bucket.
     */
    public String buildFileStatsQuery(Long tableId, String partitionName, Integer bucket, Long snapshotId) {
        return String.format(
            "SELECT %s FROM %s.%s " +
            "WHERE table_id = %d AND partition_name = '%s' AND bucket = %d AND snapshot_id = %d",
            FILE_STATS_COLUMNS,
            LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
            LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME,
            tableId,
            partitionName,
            bucket,
            snapshotId
        );
    }

    /**
     * Build SQL to query all file statistics for a given table and snapshot.
     */
    public String buildAllFileStatsQuery(Long tableId, Long snapshotId) {
        return String.format(
            "SELECT %s FROM %s.%s " +
            "WHERE table_id = %d AND snapshot_id = %d",
            FILE_STATS_COLUMNS,
            LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
            LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME,
            tableId,
            snapshotId
        );
    }

    /**
     * Build SQL to query file statistics with partition/bucket predicates.
     * Generates SQL with IN clauses for partitions and buckets.
     */
    public String buildFileStatsQueryWithPredicates(long tableId, long snapshotId,
                                                    Map<String, Set<Integer>> partitionBuckets) {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format(
                "SELECT %s FROM %s.%s WHERE table_id = %d AND snapshot_id = %d",
                FILE_STATS_COLUMNS,
                LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME,
                tableId,
                snapshotId
        ));

        if (partitionBuckets.isEmpty()) {
            return sql.toString();
        }

        // Build partition/bucket predicate
        // Strategy: (partition_name = 'p1' AND bucket IN (0,1)) OR (partition_name = 'p2' AND bucket IN (2,3))
        sql.append(" AND (");

        boolean first = true;
        for (Map.Entry<String, Set<Integer>> entry : partitionBuckets.entrySet()) {
            String partitionName = entry.getKey();
            Set<Integer> buckets = entry.getValue();

            if (!first) {
                sql.append(" OR ");
            }
            first = false;

            if (buckets == null || buckets.isEmpty()) {
                // All buckets for this partition
                sql.append(String.format("partition_name = '%s'", escapeString(partitionName)));
            } else {
                // Specific buckets
                String bucketList = buckets.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                sql.append(String.format("(partition_name = '%s' AND bucket IN (%s))",
                        escapeString(partitionName), bucketList));
            }
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Escape single quotes in string for SQL.
     */
    private String escapeString(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("'", "''");
    }

    /**
     * Build SQL to query partition statistics for a given table and snapshot.
     */
    public String buildPartitionsQuery(Long tableId, Long snapshotId) {
        return String.format(
            "SELECT table_id, snapshot_id, partition_name, partition_values, " +
            "row_count, data_size, file_count, min_key, max_key, last_file_creation_time " +
            "FROM %s.%s " +
            "WHERE table_id = %d AND snapshot_id = %d",
            LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
            LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME,
            tableId,
            snapshotId
        );
    }

    /**
     * Build SQL to query table schema by catalog/database/table name.
     */
    public String buildSchemaQueryByName(String catalogName, String databaseName, String tableName) {
        return String.format(
            "SELECT catalog_name, database_name, table_name, table_id, table_uuid, " +
            "begin_snapshot, end_snapshot, bucket_num " +
            "FROM %s.%s " +
            "WHERE catalog_name = '%s' AND database_name = '%s' AND table_name = '%s'",
            LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
            LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME,
            catalogName,
            databaseName,
            tableName
        );
    }
}

