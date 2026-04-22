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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.lakeoptimizer.query.LakeOptimizerQueryService;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TPaimonMetadataType;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.table.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LakeOptimizerCacheManagerTest {

    private LakeOptimizerCacheManager cacheManager;

    @Mocked
    private Table mockNativeTable;

    @Before
    public void setUp() {
        cacheManager = new LakeOptimizerCacheManager();
    }

    @Test
    public void testTableCacheHitAndMiss() {
        TableCacheKey key = new TableCacheKey("test_catalog", "test_db", "test_table");

        List<Column> columns = Collections.singletonList(
                new Column("id", Type.INT, true));
        PaimonTable mockTable = new PaimonTable("test_catalog", "test_db", "test_table",
                columns, mockNativeTable);

        AtomicInteger loadCount = new AtomicInteger(0);

        // First call - cache miss
        PaimonTable result1 = cacheManager.getTable(key, k -> {
            loadCount.incrementAndGet();
            return mockTable;
        });
        Assert.assertNotNull(result1);
        Assert.assertEquals(1, loadCount.get());

        // Second call - cache hit
        PaimonTable result2 = cacheManager.getTable(key, k -> {
            loadCount.incrementAndGet();
            return mockTable;
        });
        Assert.assertSame(result1, result2);
        Assert.assertEquals(1, loadCount.get());
    }

    @Test
    public void testTableCacheConcurrentAccess() throws InterruptedException {
        TableCacheKey key = new TableCacheKey("catalog", "db", "concurrent_table");
        List<Column> columns = Collections.singletonList(new Column("id", Type.INT, true));

        AtomicInteger loadCount = new AtomicInteger(0);
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    cacheManager.getTable(key, k -> {
                        loadCount.incrementAndGet();
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return new PaimonTable("catalog", "db", "concurrent_table", columns, mockNativeTable);
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        // Only one thread should invoke the loader
        Assert.assertEquals(1, loadCount.get());
    }

    @Test
    public void testInvalidateTable() {
        TableCacheKey key = new TableCacheKey("test_catalog", "test_db", "test_table");
        List<Column> columns = Collections.singletonList(new Column("id", Type.INT, true));
        PaimonTable mockTable = new PaimonTable("test_catalog", "test_db", "test_table", columns, mockNativeTable);

        cacheManager.getTable(key, k -> mockTable);
        Assert.assertEquals(1, cacheManager.getTableCacheSize());

        cacheManager.invalidateTable("test_catalog", "test_db", "test_table");
        Assert.assertEquals(0, cacheManager.getTableCacheSize());
    }

    @Test
    public void testPartitionsCacheMissAndHit() {
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };

        // First call - cache miss
        cacheManager.getPartitions(100L, 5L, "test_table");
        Assert.assertEquals(1, capturedSqls.size());
        Assert.assertTrue(capturedSqls.get(0).contains("table_id = 100"));
        Assert.assertTrue(capturedSqls.get(0).contains("snapshot_id = 5"));

        // Second call - cache hit, no additional query
        cacheManager.getPartitions(100L, 5L, "test_table");
        Assert.assertEquals(1, capturedSqls.size());
    }

    @Test
    public void testManifestCacheMissAndHit() {
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };
        new ConnectContext().setThreadLocalInfo();
        Map<String, Partition> partitionMap = createPartitionMap("dt=2024-01-01");
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        partitionBuckets.put("dt=2024-01-01", new HashSet<>(Collections.singletonList(0)));

        // First call - cache miss
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 1, false);
        Assert.assertEquals(1, capturedSqls.size());

        // Second call - cache hit
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 1, false);
        Assert.assertEquals(1, capturedSqls.size());
    }

    @Test
    public void testManifestQueryFiltered1() {
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.putAll(createPartitionMap("dt=2024-01-01"));
        partitionMap.putAll(createPartitionMap("dt=2024-01-02"));

        Set<Integer> buckets = new HashSet<>();
        buckets.add(0);
        buckets.add(2);
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        partitionBuckets.put("dt=2024-01-01", buckets);
        partitionBuckets.put("dt=2024-01-02", buckets);

        // totalPartitions=10 > partitionMap.size()=2, triggers filtered query
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 10, false);

        Assert.assertEquals(1, capturedSqls.size());
        String sql = capturedSqls.get(0);

        // Verify partition values in SQL
        Assert.assertTrue("Should contain dt=2024-01-01", sql.contains("dt=2024-01-01"));
        Assert.assertTrue("Should contain dt=2024-01-02", sql.contains("dt=2024-01-02"));

        // Verify bucket IN clause in SQL - check for "bucket IN (0,2)" or "bucket IN (2,0)"
        Assert.assertTrue("Should contain bucket IN clause",
                sql.contains("bucket IN (0,2)") || sql.contains("bucket IN (2,0)"));
    }

    @Test
    public void testManifestQueryFiltered2() {
        // Test case: non-partitioned table with bucket pruning
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };

        // For unpartitioned table, partition name is empty string
        Map<String, Partition> partitionMap = createPartitionMap("");

        Set<Integer> buckets = new HashSet<>();
        buckets.add(1);
        buckets.add(3);
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        partitionBuckets.put("", buckets);

        // totalPartitions=1 (unpartitioned), but bucket pruning is effective
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 1, true);

        Assert.assertEquals(1, capturedSqls.size());
        String sql = capturedSqls.get(0);

        // Should use filtered query because bucket pruning is effective
        Assert.assertTrue("Should contain bucket IN clause",
                sql.contains("bucket IN (1,3)") || sql.contains("bucket IN (3,1)"));
        // Should contain empty partition name
        Assert.assertTrue("Should contain partition_name filter for empty partition",
                sql.contains("partition_name = ''"));
    }

    @Test
    public void testManifestQueryFiltered3() {
        // Test case: partition pruning is effective, but buckets is null (no bucket pruning)
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };

        Map<String, Partition> partitionMap = new HashMap<>();
        partitionMap.putAll(createPartitionMap("dt=2024-01-01"));
        partitionMap.putAll(createPartitionMap("dt=2024-01-02"));

        // totalPartitions=10 > partitionMap.size()=2, triggers filtered query
        // empty partitionBuckets means no bucket pruning
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        partitionBuckets.put("dt=2024-01-01", null);
        partitionBuckets.put("dt=2024-01-02", null);
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 10, false);

        Assert.assertEquals(1, capturedSqls.size());
        String sql = capturedSqls.get(0);

        // Verify partition values in SQL
        Assert.assertTrue("Should contain dt=2024-01-01", sql.contains("dt=2024-01-01"));
        Assert.assertTrue("Should contain dt=2024-01-02", sql.contains("dt=2024-01-02"));

        // Should NOT contain bucket IN clause since buckets is null
        Assert.assertFalse("Should not contain bucket IN clause when buckets is null",
                sql.contains("bucket IN"));

        Assert.assertTrue("Should contain partition_name filter only",
                sql.contains("partition_name = 'dt=2024-01-01'"));
    }

    @Test
    public void testManifestQueryAll() {
        List<String> capturedSqls = new ArrayList<>();

        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                capturedSqls.add(sql);
                return Collections.emptyList();
            }
        };

        Map<String, Partition> partitionMap = createPartitionMap("dt=2024-01-01");

        // totalPartitions=1, no bucket pruning, triggers full query
        Map<String, Set<Integer>> partitionBuckets = new HashMap<>();
        partitionBuckets.put("dt=2024-01-01", null);
        cacheManager.getManifestEntries(100L, 5L, partitionMap, partitionBuckets, "test_table", 1, false);

        Assert.assertEquals(1, capturedSqls.size());
        String sql = capturedSqls.get(0);

        Assert.assertTrue(sql.contains("table_id = 100"));
        Assert.assertTrue(sql.contains("snapshot_id = 5"));
        // Verify no partition/bucket filter
        Assert.assertFalse("Should not contain partition_name filter", sql.contains("partition_name IN"));
        Assert.assertFalse("Should not contain bucket filter", sql.contains("bucket IN"));
    }

    private Map<String, Partition> createPartitionMap(String partitionName) {
        Map<String, Partition> map = new HashMap<>();
        Partition partition = new Partition(partitionName, System.currentTimeMillis(), 1000L, 1024L, 10L);
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, partitionName.hashCode());
        writer.complete();
        partition.setPartitionValue(binaryRow);
        map.put(partitionName, partition);
        return map;
    }
}
