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

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class CompactionSchedulerTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    @Mocked
    private DatabaseTransactionMgr dbTransactionMgr;

    @Test
    public void testDisableCompaction() {
        Config.lake_compaction_disable_ids = "23456";
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(),
                        Config.lake_compaction_disable_ids);

        Assertions.assertTrue(compactionScheduler.isTableDisabled(23456L));
        Assertions.assertTrue(compactionScheduler.isPartitionDisabled(23456L));

        compactionScheduler.disableTableOrPartitionId("34567;45678;56789");

        Assertions.assertFalse(compactionScheduler.isPartitionDisabled(23456L));
        Assertions.assertTrue(compactionScheduler.isTableDisabled(34567L));
        Assertions.assertTrue(compactionScheduler.isTableDisabled(45678L));
        Assertions.assertTrue(compactionScheduler.isPartitionDisabled(56789L));

        compactionScheduler.disableTableOrPartitionId("");
        Assertions.assertFalse(compactionScheduler.isTableDisabled(34567L));
        Config.lake_compaction_disable_ids = "";
    }

    @Test
    public void testStartCompaction() {
        OlapTable table = new LakeTable();
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = new PartitionStatistics(partition);
        Quantiles q = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScore(q);
        PartitionStatisticsSnapshot snapshot = new PartitionStatisticsSnapshot(statistics);
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, globalTransactionMgr,
                globalStateMgr, "");
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return new LocalMetastore(globalStateMgr, null, null);
            }
        };
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return new Database(100, "aaa");
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return table;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public PhysicalPartition getPhysicalPartition(long physicalPartitionId) {
                return new PhysicalPartition(123, "aaa", 123, new MaterializedIndex());
            }
        };
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertNull(compactionScheduler.startCompaction(snapshot, WarehouseManager.DEFAULT_WAREHOUSE_ID,
                WarehouseManager.DEFAULT_WAREHOUSE_ID, false));
        table.setState(OlapTable.OlapTableState.NORMAL);
        Assertions.assertNull(compactionScheduler.startCompaction(snapshot, WarehouseManager.DEFAULT_WAREHOUSE_ID,
                WarehouseManager.DEFAULT_WAREHOUSE_ID, false));
    }

    @Test
    public void testGetHistory() {
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(), "");
        new MockUp<CompactionScheduler>() {
            @Mock
            public ConcurrentHashMap<PartitionIdentifier, CompactionJob> getRunningCompactions() {
                ConcurrentHashMap<PartitionIdentifier, CompactionJob> r = new ConcurrentHashMap<>();
                Database db = new Database();
                Table table = new LakeTable();
                PartitionIdentifier partitionIdentifier1 = new PartitionIdentifier(1, 2, 3);
                PartitionIdentifier partitionIdentifier2 = new PartitionIdentifier(1, 2, 4);
                PhysicalPartition partition1 = new PhysicalPartition(123, "aaa", 123, null);
                PhysicalPartition partition2 = new PhysicalPartition(124, "bbb", 124, null);
                CompactionJob job1 = new CompactionJob(db, table, partition1, 100, false);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                CompactionJob job2 = new CompactionJob(db, table, partition2, 101, false);
                r.put(partitionIdentifier1, job1);
                r.put(partitionIdentifier2, job2);
                return r;
            }
        };

        List<CompactionRecord> list = compactionScheduler.getHistory();
        Assertions.assertEquals(2, list.size());
        Assertions.assertTrue(list.get(0).getStartTs() <= list.get(1).getStartTs());
    }

    @Test
    public void testCompactionTaskLimit() {
        CompactionScheduler compactionScheduler = new CompactionScheduler(new CompactionMgr(), null, null, null, "");

        long warehouseId = 10001L;
        int defaultValue = Config.lake_compaction_max_tasks;
        // explicitly set config to a value bigger than default -1
        Config.lake_compaction_max_tasks = 10;
        Assertions.assertEquals(10, compactionScheduler.compactionTaskLimit(warehouseId));

        // reset config to default value
        Config.lake_compaction_max_tasks = defaultValue;

        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10001L, "192.168.0.3", 9050);

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };
        mockedWarehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(b1, c1, c2));
        Assertions.assertEquals(3 * Config.lake_compaction_max_parallelism_per_cn,
                compactionScheduler.compactionTaskLimit(warehouseId));
    }


    @Test
    public void testNumRunningTasksPerWarehouse(@Mocked CompactionJob compactionJob) {
        CompactionScheduler compactionScheduler = new CompactionScheduler(new CompactionMgr(), null, null, null, "");
        // new running compactions map
        long nonExistingWarehouseId = 10000L;
        long existingWarehouseId = 10001L;
        Map<PartitionIdentifier, CompactionJob> runningCompactions = new HashMap<>();

        new Expectations() {
            {
                compactionJob.getWarehouseId();
                result = existingWarehouseId;

                compactionJob.getNumTabletCompactionTasks();
                result = 5;
            }
        };

        compactionScheduler.getRunningCompactions().put(new PartitionIdentifier(1, 2, 4), compactionJob);

        Assertions.assertEquals(0, compactionScheduler.numRunningTasks(nonExistingWarehouseId));
        Assertions.assertEquals(5, compactionScheduler.numRunningTasks(existingWarehouseId));
    }

    @Test
    public void testGetCompactionWarehouseId() {
        boolean defaultValue = Config.lake_enable_bind_compaction_with_load_warehouse;

        CompactionMgr compactionMgr = new CompactionMgr();
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionMgr, null, null, null, "");
        long dbId = 1000L;
        long tableId = 1001L;
        long partitionId = 1002L;
        long loadWarehouseId = 1003L;

        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(dbId, tableId, partitionId);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier);
        statistics.setCompactionScore(Quantiles.compute(Lists.newArrayList(1.0, 2.0, 3.0)));

        ConcurrentHashMap<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap = new ConcurrentHashMap<>();
        partitionStatisticsHashMap.put(partitionIdentifier, statistics);
        Deencapsulation.setField(compactionMgr, "partitionStatisticsHashMap", partitionStatisticsHashMap);

        PartitionStatisticsSnapshot partitionStatisticsSnapshot = new PartitionStatisticsSnapshot(statistics);

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        { // case 1: disable binding, return default warehouse id
            Config.lake_enable_bind_compaction_with_load_warehouse = false;
            long compactionWarehouseId = compactionScheduler.getCompactionWarehouseId(partitionStatisticsSnapshot);
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, compactionWarehouseId);
            Config.lake_enable_bind_compaction_with_load_warehouse = defaultValue;
        }

        Config.lake_enable_bind_compaction_with_load_warehouse = true;

        { // case 2: enable binding, but load warehouse id is not set (`statistics` has warehouseId set to -1 by default)
            long compactionWarehouseId = compactionScheduler.getCompactionWarehouseId(partitionStatisticsSnapshot);
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, compactionWarehouseId);
        }

        { // case 3: enable binding, but warehouse not exists
            Config.lake_enable_bind_compaction_with_load_warehouse = true;
            statistics.setWarehouseId(loadWarehouseId);
            mockedWarehouseManager.setWarehouseExisted(false);
            long compactionWarehouseId = compactionScheduler.getCompactionWarehouseId(partitionStatisticsSnapshot);
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, compactionWarehouseId);
        }

        { // case 4: enable binding, and warehouse exists
            Config.lake_enable_bind_compaction_with_load_warehouse = true;
            statistics.setWarehouseId(loadWarehouseId);
            mockedWarehouseManager.setWarehouseExisted(true);
            long compactionWarehouseId = compactionScheduler.getCompactionWarehouseId(partitionStatisticsSnapshot);
            Assertions.assertEquals(loadWarehouseId, compactionWarehouseId);
        }

        Config.lake_enable_bind_compaction_with_load_warehouse = defaultValue;
    }

    @Test
    public void testCollectPartitionTablets() {
        CompactionScheduler compactionScheduler = new CompactionScheduler(new CompactionMgr(), null, null, null, "");

        long warehouseId = 1001L;
        long indexId = 2000L;
        long partitionId = 1000L;
        long physicalPartitionId = 5100L;
        long tabletId1 = 2000L; // assigned to compute node 1
        long tabletId2 = 2001L; // assigned to compute node 2
        long tabletId3 = 2002L; // no compute node assigned
        long tabletId4 = 2003L; // can't get compute node assigned, will throw exception

        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return new WarehouseManager();
            }
        };

        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10002L, "192.168.0.3", 9050);
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(Long warehouseId, LakeTablet tablet) {
                if (tablet.getId() == tabletId1) {
                    return c1;
                }
                if (tablet.getId() == tabletId2) {
                    return c2;
                }
                if (tablet.getId() == tabletId3) {
                    return null;
                }
                throw new IllegalArgumentException("Unknown tablet id: " + tablet.getId());
            }
        };

        { // normal case
            MaterializedIndex index1 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            Tablet tablet1 = new LakeTablet(tabletId1);
            index1.addTablet(tablet1, null, false);
            Tablet tablet2 = new LakeTablet(tabletId2);
            index1.addTablet(tablet2, null, false);

            Partition partition1 = new Partition(partitionId, partitionId + 100, "p1", index1, null);
            PhysicalPartition p10 = new PhysicalPartition(physicalPartitionId, "p10", partitionId, index1);
            partition1.addSubPartition(p10);

            Map<Long, List<String>> tabletsToPeerCacheNode = new HashMap<>();
            Map<Long, List<Long>> resultMap = compactionScheduler.collectPartitionTablets(p10, warehouseId,
                    warehouseId, tabletsToPeerCacheNode, false);
            Assertions.assertEquals(2, resultMap.size());
            List<Long> tabletListOnC1 = resultMap.get(c1.getId());
            Assertions.assertEquals(1, tabletListOnC1.size());
            Assertions.assertEquals(tabletId1, tabletListOnC1.get(0).longValue());

            List<Long> tabletListOnC2 = resultMap.get(c2.getId());
            Assertions.assertEquals(1, tabletListOnC2.size());
            Assertions.assertEquals(tabletId2, tabletListOnC2.get(0).longValue());
        }

        { // bad case 1: no compute node found for some tablet
            MaterializedIndex index2 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            Tablet tablet1 = new LakeTablet(tabletId1);
            index2.addTablet(tablet1, null, false);
            Tablet tablet2 = new LakeTablet(tabletId2);
            index2.addTablet(tablet2, null, false);
            Tablet tablet3 = new LakeTablet(tabletId3);
            index2.addTablet(tablet3, null, false);

            Partition partition2 = new Partition(partitionId, partitionId + 100, "p2", index2, null);
            PhysicalPartition p20 = new PhysicalPartition(physicalPartitionId, "p20", partitionId, null);
            partition2.addSubPartition(p20);

            Map<Long, List<String>> tabletsToPeerCacheNode = new HashMap<>();
            Map<Long, List<Long>> resultMap = compactionScheduler.collectPartitionTablets(p20, warehouseId,
                    warehouseId, tabletsToPeerCacheNode, false);
            // no compute node for tablet3
            Assertions.assertEquals(0, resultMap.size());
        }

        { // bad case 2: exception threw when getting some tablet's compute node
            MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            Tablet tablet1 = new LakeTablet(tabletId1);
            index.addTablet(tablet1, null, false);
            Tablet tablet2 = new LakeTablet(tabletId2);
            index.addTablet(tablet2, null, false);
            Tablet tablet4 = new LakeTablet(tabletId4);
            index.addTablet(tablet4, null, false);

            Partition partition = new Partition(partitionId, partitionId + 100, "p2", index, null);
            PhysicalPartition p20 = new PhysicalPartition(physicalPartitionId, "p20", partitionId, null);
            partition.addSubPartition(p20);

            Map<Long, List<String>> tabletsToPeerCacheNode = new HashMap<>();
            Map<Long, List<Long>> resultMap = compactionScheduler.collectPartitionTablets(p20, warehouseId,
                    warehouseId, tabletsToPeerCacheNode, false);
            // exception found for tablet4
            Assertions.assertEquals(0, resultMap.size());
        }
    }

    @Test
    public void testAbortStaleCompaction() {
        CompactionMgr compactionManager = new CompactionMgr();

        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        compactionManager.handleLoadingFinished(partition1, 10, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(10d)), WarehouseManager.DEFAULT_WAREHOUSE_ID);
        compactionManager.handleLoadingFinished(partition2, 10, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(10d)), WarehouseManager.DEFAULT_WAREHOUSE_ID);

        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10002L, "192.168.0.3", 9050);

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public boolean isReady() {
                return true;
            }
        };
        mockedWarehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(c1, c2));

        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, globalTransactionMgr,
                globalStateMgr, "");

        new MockUp<CompactionScheduler>() {
            @Mock
            protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot,
                                                    long warehouseId, long scheduledWarehouseId,
                                                    boolean enableCompactionService) {
                Database db = new Database();
                Table table = new LakeTable();
                long partitionId = partitionStatisticsSnapshot.getPartition().getPartitionId();
                PhysicalPartition partition = new PhysicalPartition(partitionId, "aaa", partitionId, null);
                return new CompactionJob(db, table, partition, 100, false, scheduledWarehouseId);
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(2, compactionScheduler.getRunningCompactions().size());

        CompactionScheduler.PARTITION_CLEAN_INTERVAL_SECOND = 0;
        new MockUp<MetaUtils>() {
            @Mock
            public static boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
                return false;
            }
        };
        new MockUp<CompactionJob>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NONE_SUCCESS;
            }

            @Mock
            public String getFailMessage() {
                return "abort in test";
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size());
    }

    @Test
    public void testTryCompactionSchedule(@Mocked CompactionMgr compactionMgr) {
        long testWhId = 100L;
        long bindingWhId = 101L;
        long defaultWhId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1L, 2L, 3L);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier);
        statistics.setWarehouseId(bindingWhId);
        statistics.setCompactionScore(Quantiles.compute(Lists.newArrayList(1.0, 2.0, 3.0)));
        List<PartitionStatisticsSnapshot> partitionStatisticsSnapshots = new ArrayList<>();
        PartitionStatisticsSnapshot partitionStatisticsSnapshot = new PartitionStatisticsSnapshot(statistics);
        partitionStatisticsSnapshots.add(partitionStatisticsSnapshot);
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };
        new Expectations() {
            {
                compactionMgr.choosePartitionsToCompact((Set<PartitionIdentifier>) any, (Set<Long>) any);
                result = partitionStatisticsSnapshots;
                compactionMgr.getStatistics(partitionIdentifier);
                result = statistics;
            }
        };
        CompactionScheduler compactionScheduler = new MockedCompactionScheduler(compactionMgr, null, null, null, "");
        {
            // disable compaction
            Config.lake_compaction_max_tasks = 0;
            new Expectations() {
                {
                    compactionMgr.choosePartitionsToCompact((Set<PartitionIdentifier>) any, (Set<Long>) any);
                    times = 0;
                }
            };
            compactionScheduler.tryCompactionSchedule();
        }
        {
            Config.lake_compaction_max_tasks = -1;
            new Expectations() {
                {
                    compactionMgr.choosePartitionsToCompact((Set<PartitionIdentifier>) any, (Set<Long>) any);
                    result = partitionStatisticsSnapshots;
                }
            };
            Config.lake_enable_bind_compaction_with_load_warehouse = false;
            compactionScheduler.getRunningCompactions().clear();
            compactionScheduler.tryCompactionSchedule();
            assertEquals(1, compactionScheduler.getRunningCompactions().size());
            // not set `lake_enable_bind_compaction_with_load_warehouse` and `lake_compaction_warehouse`, use default_warehouse
            assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                    compactionScheduler.getRunningCompactions().get(partitionIdentifier).getWarehouseId());
            mockedWarehouseManager.setCompactionWarehouseID(testWhId);
            compactionScheduler.getRunningCompactions().clear();
            Config.lake_enable_bind_compaction_with_load_warehouse = false;
            compactionScheduler.tryCompactionSchedule();
            // `lake_enable_bind_compaction_with_load_warehouse` disabled, use compaction warehouse configured by FE
            assertEquals(testWhId, compactionScheduler.getRunningCompactions().get(partitionIdentifier).getWarehouseId());
            mockedWarehouseManager.setWarehouseExisted(true);
            compactionScheduler.getRunningCompactions().clear();
            Config.lake_enable_bind_compaction_with_load_warehouse = true;
            compactionScheduler.tryCompactionSchedule();
            // `lake_enable_bind_compaction_with_load_warehouse` enabled, compaction job should bind the loading warehouse
            assertEquals(bindingWhId,
                    compactionScheduler.getRunningCompactions().get(partitionIdentifier).getWarehouseId());
        }
    }

    @Test
    public void testCompactionServiceWarehouseConsistency(@Mocked CompactionMgr compactionMgr) {
        // This test verifies that when compaction service is enabled,
        // the CompactionJob records the correct compaction service warehouse ID
        // and this ID remains consistent throughout the compaction lifecycle.
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();

        long originWarehouseId = 2000L;
        long compactionServiceWarehouseId = 3000L;
        long partitionId = 1000L;

        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, partitionId);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier);
        statistics.setWarehouseId(originWarehouseId);
        Quantiles q = Quantiles.compute(Lists.newArrayList(1.0, 2.0, 3.0));
        statistics.setCompactionScore(q);
        PartitionStatisticsSnapshot snapshot = new PartitionStatisticsSnapshot(statistics);

        List<PartitionStatisticsSnapshot> partitionStatisticsSnapshots = new ArrayList<>();
        partitionStatisticsSnapshots.add(snapshot);

        // Mock GlobalStateMgr to return our mocked warehouse manager
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        // Mock warehouse manager to return a specific compaction service warehouse
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getCompactionServiceWarehouse() {
                return new DefaultWarehouse(compactionServiceWarehouseId, "compaction_service_warehouse");
            }
            
            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                if (warehouseId == originWarehouseId) {
                    return new DefaultWarehouse(originWarehouseId, "origin_warehouse");
                } else if (warehouseId == compactionServiceWarehouseId) {
                    return new DefaultWarehouse(compactionServiceWarehouseId, "compaction_service_warehouse");
                }
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, 
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
            
            @Mock
            public boolean warehouseExists(long warehouseId) {
                return true;
            }
        };

        // Setup expectations for the mocked CompactionMgr
        new Expectations() {
            {
                compactionMgr.choosePartitionsToCompact((Set<PartitionIdentifier>) any, (Set<Long>) any);
                result = partitionStatisticsSnapshots;
                compactionMgr.getStatistics(partitionIdentifier);
                result = statistics;
            }
        };

        MockedCompactionScheduler compactionScheduler = new MockedCompactionScheduler(
                compactionMgr, null, null, null, "");

        // Case 1: With compaction service enabled, should use compaction service warehouse
        Config.enable_lake_compaction_service = true;
        Config.lake_compaction_max_tasks = -1;
        Config.lake_enable_bind_compaction_with_load_warehouse = true;
        mockedWarehouseManager.setWarehouseExisted(true);

        compactionScheduler.getRunningCompactions().clear();
        compactionScheduler.tryCompactionSchedule();

        // Verify that the CompactionJob uses compaction service warehouse ID
        assertEquals(1, compactionScheduler.getRunningCompactions().size());
        CompactionJob job = compactionScheduler.getRunningCompactions().get(partitionIdentifier);
        Assertions.assertNotNull(job);
        // The warehouse ID should be compaction service warehouse, not the origin warehouse
        assertEquals("CompactionJob should record compaction service warehouse ID when compaction service is enabled",
                compactionServiceWarehouseId, job.getWarehouseId());
        Assertions.assertNotEquals(originWarehouseId, job.getWarehouseId(),
                "CompactionJob should not use origin warehouse ID when compaction service is enabled");

        // Case 2: With compaction service disabled, should use origin warehouse (bind warehouse)
        Config.enable_lake_compaction_service = false;
        compactionScheduler.getRunningCompactions().clear();
        
        // Setup expectations again for the second call
        new Expectations() {
            {
                compactionMgr.choosePartitionsToCompact((Set<PartitionIdentifier>) any, (Set<Long>) any);
                result = partitionStatisticsSnapshots;
                compactionMgr.getStatistics(partitionIdentifier);
                result = statistics;
            }
        };
        
        compactionScheduler.tryCompactionSchedule();

        assertEquals(1, compactionScheduler.getRunningCompactions().size());
        job = compactionScheduler.getRunningCompactions().get(partitionIdentifier);
        Assertions.assertNotNull(job);
        // When compaction service is disabled, should use the binding warehouse
        assertEquals("CompactionJob should use origin warehouse ID when compaction service is disabled",
                originWarehouseId, job.getWarehouseId());

        // Reset config
        Config.enable_lake_compaction_service = false;
        Config.lake_enable_bind_compaction_with_load_warehouse = false;
    }
}

class MockedCompactionScheduler extends CompactionScheduler {
    MockedCompactionScheduler(CompactionMgr compactionManager, SystemInfoService systemInfoService,
                              GlobalTransactionMgr transactionMgr,
                              GlobalStateMgr stateMgr, String disableTablesStr) {
        super(compactionManager, systemInfoService, transactionMgr, stateMgr, disableTablesStr);
    }
    @Override
    protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot, long warehouseId,
                                            long scheduledWarehouseId, boolean enableCompactionService) {
        Database db = new Database();
        Table table = new LakeTable();
        PhysicalPartition partition = new PhysicalPartition(123, "aaa", 123, null);
        return new CompactionJob(db, table, partition, 100, false, scheduledWarehouseId);
    }
}
