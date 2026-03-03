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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class CatalogRecycleBinLakeTableDropTableTest extends CatalogRecycleBinLakeTableTest {
    private static final Logger LOG = LogManager.getLogger(CatalogRecycleBinLakeTableDropTableTest.class);

    /**
     * Test erasing a Lake Table that has no partitions.
     * This covers the code path where addLakeTablePartitionsToRecycleBin returns false,
     * and the table is directly finished in a single eraseTable() call.
     */
    @Test
    public void testEraseLakeTableWithNoPartitions(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_table_no_partitions_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a range-partitioned table with one partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Assertions.assertNotNull(p1);

        // Drop partition first (force), so the table becomes empty
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1 FORCE", dbName));
        Assertions.assertNull(table.getPartition("p1"));

        // Force drop the now-empty table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        // One dropTable call expected for the individually dropped partition p1
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // Erase the individually dropped partition p1 first
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);
        recycleBin.erasePartition(futureTime);

        // eraseTable should handle the empty table in a single call:
        // addLakeTablePartitionsToRecycleBin returns false (no partitions),
        // so table is directly finished without partition-level deletion.
        recycleBin.eraseTable(futureTime);

        // Table should be erased
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        // No partition tracking should exist
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        // Tablet meta should be removed
        checkTableTablet(table, false);
    }

    /**
     * Test that replayEraseTable properly cleans up lakeTableToPartitions and partitionsFromTableDeletion.
     * This covers the cleanup code in removeTableFromRecycleBin() that was added for lake table support.
     */
    @Test
    public void testReplayEraseTableCleansUpPartitionTracking(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "replay_erase_cleanup_tracking_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);

        // Verify tracking data exists
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p2.getId()));

        // Simulate follower replay: replayEraseTable should clean up all tracking data
        recycleBin.replayEraseTable(Lists.newArrayList(table.getId()));

        // Verify table is removed
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        // Verify all tracking data is cleaned up
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        // Verify partitions are removed from idToPartition
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        // Verify recycle times are cleaned
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(p1.getId()));
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(p2.getId()));
    }

    /**
     * Test that eraseTable() does nothing when partitions are still being deleted asynchronously.
     * This covers the "do nothing and wait for next cycle" path in eraseTable().
     */
    @Test
    public void testEraseTableWaitsForPartitionsStillDeleting(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_table_waits_for_partitions_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 2 dropTable calls expected for the 2 partitions
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Second eraseTable call: partitions are still in idToPartition (not processed yet)
        // eraseTable should do nothing and wait
        recycleBin.eraseTable(futureTime);
        // Table should still exist
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Now process the partitions
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);
        recycleBin.erasePartition(futureTime);

        // All partitions should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Third eraseTable call: all partitions deleted, should clean up and finish
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
    }

    /**
     * Test that erasePartition handles CompletableFuture.get() exception correctly for
     * partitions from table deletion. The partition should remain in the recycle bin for retry.
     */
    @Test
    public void testErasePartitionFromTableDeletionWithException(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_partition_table_deletion_exception_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 1 partition
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First call throws exception, second call succeeds
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = new RuntimeException("mocked RPC exception"); // throws exception
                result = buildDropTableResponse(0, ""); // succeeds on retry
            }
        };

        // First eraseTable call: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));

        // First erasePartition: submits async task which will throw exception
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);

        // Second erasePartition: processes completed task with exception
        // future.get() throws ExecutionException wrapping RuntimeException
        // finished stays false, asyncDeleteForPartitions is cleaned up for retry
        recycleBin.erasePartition(futureTime);

        // Partition should still be pending (failed, not removed)
        Assertions.assertEquals(1, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));

        // Retry: submit new async task
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);
        recycleBin.erasePartition(futureTime);

        // Now partition should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
    }

    /**
     * Test erasing a lake table that is unpartitioned (single default partition).
     * This covers the RecycleLakeUnPartitionInfo.delete() with forceRemoveDirectory=true path.
     */
    @Test
    public void testEraseLakeUnpartitionedTable(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_unpartitioned_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create an unpartitioned table
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 INT," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        // Unpartitioned table has exactly one partition
        Assertions.assertEquals(1, table.getPartitions().size());
        Partition defaultPartition = table.getPartitions().iterator().next();
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 1 dropTable call expected for the single partition
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds the single partition to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(1, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(defaultPartition.getId()));
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(defaultPartition.getId()));

        // erasePartition processes the partition (RecycleLakeUnPartitionInfo.delete())
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);
        recycleBin.erasePartition(futureTime);

        // Partition should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        checkTableTablet(table, false);
    }

    /**
     * Test erasing a lake table with list partitions.
     * This covers the RecycleLakeListPartitionInfo.delete() with forceRemoveDirectory=true path.
     */
    @Test
    public void testEraseLakeListPartitionedTable(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "erase_lake_list_partitioned_table_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a list-partitioned table
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE NOT NULL," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY LIST(k1) (" +
                        "  PARTITION p1 VALUES IN ('2024-01-01')," +
                        "  PARTITION p2 VALUES IN ('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        // Exactly 2 dropTable calls expected for the 2 list partitions
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 2;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds list partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(2, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(p2.getId()));

        // erasePartition processes the partitions (RecycleLakeListPartitionInfo.delete())
        recycleBin.erasePartition(futureTime);
        Thread.sleep(500);
        recycleBin.erasePartition(futureTime);

        // Partitions should be deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Clean up table
        recycleBin.eraseTable(futureTime);
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        checkTableTablet(table, false);
    }

    /**
     * Test that RecyclePartitionInfo.forceRemoveDirectory defaults to false.
     * Partitions dropped individually (not as part of table deletion) should not
     * have forceRemoveDirectory set, so shared directories are skipped.
     */
    @Test
    public void testPartitionDroppedIndividuallyHasForceRemoveDirectoryFalse(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "partition_individual_drop_no_force_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Assertions.assertNotNull(p1);

        // Drop partition individually (force)
        alterTable(connectContext, String.format("ALTER TABLE %s.t1 DROP PARTITION p1 FORCE", dbName));
        Assertions.assertNull(table.getPartition("p1"));
        Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()));

        // Verify forceRemoveDirectory is false (default) for individually dropped partition
        Assertions.assertFalse(recycleBin.isPartitionForceRemoveDirectory(p1.getId()),
                "Individually dropped partition should have forceRemoveDirectory=false");
        // Verify it's NOT marked as from table deletion
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()),
                "Individually dropped partition should not be marked as from table deletion");

        // Clean up
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        // Exactly 1 dropTable call expected for the individually dropped partition
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 1;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        waitPartitionClearFinished(recycleBin, p1.getId(), System.currentTimeMillis() + delay);
    }

    /**
     * Test that removeTableFromRecycleBin (called during replay or erase) properly cleans up
     * async delete futures for lake table partitions along with other tracking data.
     */
    @Test
    public void testRemoveTableFromRecycleBinCleansAsyncDeleteFutures(
            @Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "remove_table_cleans_async_futures_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 2 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // eraseTable: adds partitions to tracking
        recycleBin.eraseTable(futureTime);
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));

        // erasePartition: submits async tasks (futures are created)
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                minTimes = 0;
                result = buildDropTableResponse(0, "");
            }
        };
        recycleBin.erasePartition(futureTime);

        // At this point, asyncDeleteForPartitions should have entries
        Assertions.assertTrue(containsAsyncDeletePartition(recycleBin, p1.getId()) ||
                        containsAsyncDeletePartition(recycleBin, p2.getId()),
                "At least one partition should have an async delete future");

        // removeTableFromRecycleBin should clean up everything including async futures
        recycleBin.removeTableFromRecycleBin(Lists.newArrayList(table.getId()));

        // Verify all tracking is cleaned up
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p1.getId()));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(p2.getId()));
        Assertions.assertFalse(containsAsyncDeletePartition(recycleBin, p1.getId()));
        Assertions.assertFalse(containsAsyncDeletePartition(recycleBin, p2.getId()));
    }


    /**
     * Test that force dropping a Lake Table with shared partition directories
     * properly cleans up the shared directories. Without the forceRemoveDirectory flag,
     * shared directories would be skipped by removePartitionDirectory().
     */
    @Test
    public void testForceDropLakeTableCleansSharedDirectories(@Mocked LakeService lakeService) throws Exception {
        LOG.warn("Start test: {}, lakeService={}", currentCaseName, lakeService);
        final String dbName = "force_drop_shared_directory_test";
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // Create database
        String createDbStmtStr = String.format("create database %s;", dbName);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

        // Create a table with 3 partitions
        Table table = createTable(connectContext, String.format(
                "CREATE TABLE %s.t1" +
                        "(" +
                        "  k1 DATE," +
                        "  v1 varchar(10)" +
                        ")" +
                        "DUPLICATE KEY(k1)\n" +
                        "PARTITION BY RANGE(k1) (" +
                        "  PARTITION p1 VALUES LESS THAN('2024-01-01')," +
                        "  PARTITION p2 VALUES LESS THAN('2024-02-01')," +
                        "  PARTITION p3 VALUES LESS THAN('2024-03-01')" +
                        ")" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES('replication_num' = '1');", dbName));

        Assertions.assertTrue(table.isCloudNativeTable());
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        Partition p3 = table.getPartition("p3");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        Assertions.assertNotNull(p3);
        checkTableTablet(table, true);

        // Force drop the table
        dropTable(connectContext, String.format("DROP TABLE %s.t1 FORCE", dbName));
        Assertions.assertNotNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isTableRecoverable(db.getId(), table.getId()));

        // Mock isSharedDirectory to always return true (simulating all partitions share a directory).
        // With forceRemoveDirectory=true (set during table deletion), isSharedDirectory should NOT
        // even be called because the && short-circuits in removePartitionDirectory.
        AtomicBoolean isSharedDirectoryCalled = new AtomicBoolean(false);
        new MockUp<LakeTableHelper>() {
            @Mock
            public boolean isSharedDirectory(String path, long partitionId) {
                isSharedDirectoryCalled.set(true);
                return true; // all dirs are "shared"
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) throws RpcException {
                return lakeService;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public ComputeResource getCurrentComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };

        // All 3 partitions should have their directories removed even though they are "shared",
        // because forceRemoveDirectory=true bypasses the shared directory check.
        new Expectations() {
            {
                lakeService.dropTable((DropTableRequest) any);
                times = 3;
                result = buildDropTableResponse(0, "");
            }
        };

        long delay = Math.max(Config.catalog_trash_expire_second * 1000, CatalogRecycleBin.getMinEraseLatency()) + 1;
        long futureTime = System.currentTimeMillis() + delay;

        // First eraseTable call: adds partitions with forceRemoveDirectory=true
        recycleBin.eraseTable(futureTime);

        // Verify partitions are tracked and have forceRemoveDirectory set
        Assertions.assertTrue(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        Assertions.assertEquals(3, recycleBin.getLakeTablePendingPartitionCount(table.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p2.getId()));
        Assertions.assertTrue(recycleBin.isPartitionFromTableDeletion(p3.getId()));
        // Verify forceRemoveDirectory is set on each partition
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(p1.getId()));
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(p2.getId()));
        Assertions.assertTrue(recycleBin.isPartitionForceRemoveDirectory(p3.getId()));

        // erasePartition processes the partitions
        recycleBin.erasePartition(futureTime);
        // Wait for async deletion to complete
        Thread.sleep(500);
        // Second erasePartition call to process completed async tasks
        recycleBin.erasePartition(futureTime);

        // Verify all partitions are deleted
        Assertions.assertEquals(0, recycleBin.getLakeTablePendingPartitionCount(table.getId()));

        // Verify isSharedDirectory was NOT called (short-circuited by forceRemoveDirectory=true)
        Assertions.assertFalse(isSharedDirectoryCalled.get(),
                "isSharedDirectory should not be called when forceRemoveDirectory is true");

        // Final eraseTable call: cleans up table
        recycleBin.eraseTable(futureTime);

        // Table should be fully erased
        Assertions.assertNull(recycleBin.getTable(db.getId(), table.getId()));
        Assertions.assertFalse(recycleBin.isLakeTablePartitionsDeletionInProgress(table.getId()));
        checkTableTablet(table, false);
    }
}
