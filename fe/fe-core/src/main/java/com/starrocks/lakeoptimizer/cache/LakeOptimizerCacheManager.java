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

package com.starrocks.lakeoptimizer.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.lakeoptimizer.query.LakeOptimizerQueryService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.manifest.ManifestEntryWithDeletionFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/**
 * Cache manager for LakeOptimizer metadata.
 */
public class LakeOptimizerCacheManager {
    private static final Logger LOG = LogManager.getLogger(LakeOptimizerCacheManager.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";
    private static final String TRACE_PREFIX = "Paimon.LakeOptimizer.";

    private final LakeOptimizerQueryService queryService;

    // Table cache: key = (catalog, db, table)
    private final Cache<TableCacheKey, PaimonTable> tableCache;

    // Partition cache: key = (tableId, snapshotId), value = List<Partition>
    private final Cache<PartitionCacheKey, List<Partition>> partitionCache;

    // Manifest cache: key = (tableId, snapshotId, partition, bucket), value = List<ManifestEntryWithDeletionFile>
    private final Cache<ManifestCacheKey, List<ManifestEntryWithDeletionFile>> manifestCache;

    public LakeOptimizerCacheManager() {
        this.queryService = new LakeOptimizerQueryService();

        this.tableCache = Caffeine.newBuilder()
                .maximumSize(Config.lake_optimizer_table_cache_max_size)
                .expireAfterWrite(Config.lake_optimizer_table_cache_expire_sec, TimeUnit.SECONDS)
                .build();

        this.partitionCache = Caffeine.newBuilder()
                .maximumSize(Config.lake_optimizer_partition_cache_max_size)
                .expireAfterWrite(Config.lake_optimizer_partition_cache_expire_sec, TimeUnit.SECONDS)
                .build();

        this.manifestCache = Caffeine.newBuilder()
                .maximumSize(Config.lake_optimizer_manifest_cache_max_size)
                .expireAfterWrite(Config.lake_optimizer_manifest_cache_expire_sec, TimeUnit.SECONDS)
                .build();

        LOG.info("{} Initialized CacheManager: tableCache(maxSize={}, expireSec={}), " +
                        "partitionCache(maxSize={}, expireSec={}), manifestCache(maxSize={}, expireSec={})",
                LOG_PREFIX,
                Config.lake_optimizer_table_cache_max_size, Config.lake_optimizer_table_cache_expire_sec,
                Config.lake_optimizer_partition_cache_max_size, Config.lake_optimizer_partition_cache_expire_sec,
                Config.lake_optimizer_manifest_cache_max_size, Config.lake_optimizer_manifest_cache_expire_sec);
    }

    public PaimonTable getTable(TableCacheKey key, Function<TableCacheKey, PaimonTable> loader) {
        return tableCache.get(key, k -> {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, TRACE_PREFIX + key.tableName + ".tableLoadTime")) {
                return loader.apply(k);
            }
        });
    }

    public void invalidateTable(String catalogName, String dbName, String tableName) {
        TableCacheKey key = new TableCacheKey(catalogName, dbName, tableName);
        tableCache.invalidate(key);
    }

    public List<Partition> getPartitions(long tableId, long snapshotId, String tableName) {
        PartitionCacheKey key = new PartitionCacheKey(tableId, snapshotId);

        List<Partition> result = partitionCache.get(key, k -> {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, TRACE_PREFIX + tableName + ".partitionLoadTime")) {
                List<Partition> partitions = queryService.queryPartitions(tableId, snapshotId);
                partitions = partitions != null ? partitions : new ArrayList<>();
                return partitions;
            }
        });

        Tracers.record(EXTERNAL, TRACE_PREFIX + tableName + ".partitionCount",
                String.valueOf(result != null ? result.size() : 0));

        return result;
    }

    /**
     * Get manifest entries with deletion files for the specified partitions and buckets.
     *
     * @param tableId Table ID in entity table
     * @param snapshotId Snapshot ID to query
     * @param partitionMap Map of partition name to Partition object (contains BinaryRow for ManifestEntry).
     *                     For unpartitioned tables, contains a single entry with empty partition name.
     *                     For partitioned tables, contains all partition entries.
     * @param prunedBuckets Set of bucket IDs after bucket pruning, or null if bucket pruning is not applicable.
     * @param tableName Table name for logging and tracing
     * @param totalPartitions Total partition count (used to decide if partition pruning is effective)
     * @param isUnpartitioned Whether the table is unpartitioned
     * @return List of ManifestEntryWithDeletionFile for the queried partitions/buckets
     */
    public List<ManifestEntryWithDeletionFile> getManifestEntries(long tableId, long snapshotId,
                                                                Map<String, Partition> partitionMap,
                                                   Set<Integer> prunedBuckets,
                                                   String tableName,
                                                   int totalPartitions,
                                                   boolean isUnpartitioned) {
        List<ManifestEntryWithDeletionFile> result;
        // Single partition + single bucket: use cache for point query optimization
        if (isSingleKey(partitionMap, prunedBuckets)) {
            result = getManifestEntriesSingle(tableId, snapshotId, partitionMap, prunedBuckets,
                    tableName);
        } else {
            result = getManifestEntriesInternal(tableId, snapshotId, partitionMap, prunedBuckets,
                    tableName, totalPartitions, isUnpartitioned);
        }
        Tracers.record(EXTERNAL, TRACE_PREFIX + tableName + ".fileCount", String.valueOf(result.size()));
        return result;
    }

    /**
     * Check if the request is for a single partition + single bucket.
     */
    private boolean isSingleKey(Map<String, Partition> partitionMap, Set<Integer> prunedBuckets) {
        return partitionMap.size() == 1 && prunedBuckets != null && prunedBuckets.size() == 1;
    }

    /**
     * Get manifest entries with deletion files for single partition + single bucket.
     */
    private List<ManifestEntryWithDeletionFile> getManifestEntriesSingle(long tableId, long snapshotId,
                                                          Map<String, Partition> partitionMap,
                                                          Set<Integer> prunedBuckets,
                                                          String tableName) {
        Map.Entry<String, Partition> entry = partitionMap.entrySet().iterator().next();
        String partitionName = entry.getKey();
        Partition partition = entry.getValue();
        int bucket = prunedBuckets.iterator().next();
        ManifestCacheKey key = new ManifestCacheKey(tableId, snapshotId, partitionName, bucket);
        return manifestCache.get(key, k -> {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, TRACE_PREFIX + tableName + ".fileQueryTime")) {
                List<ManifestEntryWithDeletionFile> entries = queryService.queryManifestSingleKey(
                        tableId, snapshotId, partition, bucket);
                entries = entries != null ? entries : new ArrayList<>();
                LOG.debug("{} Manifest cache MISS: key={}, count={}", LOG_PREFIX, key, entries.size());
                return entries;
            }
        });
    }

    /**
     * Get manifest entries with deletion files by querying `file_statistics` table directly.
     */
    private List<ManifestEntryWithDeletionFile> getManifestEntriesInternal(long tableId, long snapshotId,
                                                            Map<String, Partition> partitionMap,
                                                            Set<Integer> prunedBuckets,
                                                            String tableName,
                                                            int totalPartitions,
                                                            boolean isUnpartitioned) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, TRACE_PREFIX + tableName + ".fileQueryTime")) {
            // If partition or bucket prune is effective, query partial
            boolean queryPartial = (partitionMap.size() < totalPartitions) || (prunedBuckets != null);

            List<ManifestEntryWithDeletionFile> entries;
            if (queryPartial) {
                Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
                for (String partitionName : partitionMap.keySet()) {
                    partitionBuckets.put(partitionName, prunedBuckets);
                }
                entries = queryService.queryManifestFiltered(
                        tableId, snapshotId, partitionMap, partitionBuckets, isUnpartitioned);
            } else {
                entries = queryService.queryManifestAll(
                        tableId, snapshotId, partitionMap, isUnpartitioned);
            }
            LOG.debug("{} Manifest query: table={}, partitions={}, count={}",
                    LOG_PREFIX, tableName, partitionMap.size(), entries.size());
            return entries;
        }
    }

    public LakeOptimizerQueryService getQueryService() {
        return queryService;
    }

    public long getTableCacheSize() {
        return tableCache.estimatedSize();
    }
}
