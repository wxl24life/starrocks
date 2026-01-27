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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LocalMetastore#addPhysicalPartition} method.
 */
public class LocalMetastoreAddPhysicalPartitionTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test_db");
        starRocksAssert.useDatabase("test_db");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        try {
            starRocksAssert.dropDatabase("test_db");
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Test adding physical partition to a non-partitioned table with random distribution.
     * This tests the success scenario where:
     * - Table uses random distribution
     * - No partition name is specified (non-partitioned table)
     * - Custom bucket number is provided
     */
    @Test
    public void testAddPhysicalPartitionForNonPartitionedTable() throws Exception {
        String tableName = "test_non_partitioned_random";
        String dropSQL = "drop table if exists " + tableName;
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

        // Create a non-partitioned table with random distribution
        String createSQL = "CREATE TABLE test_db." + tableName + " (\n" +
                "    k1 INT,\n" +
                "    k2 VARCHAR(50),\n" +
                "    v1 INT\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 8\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_db");
        OlapTable table = (OlapTable) db.getTable(tableName);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(1, table.getPhysicalPartitions().size());

        // Add physical partition without specifying partition name (for non-partitioned table)
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPhysicalPartition(db, table, null, 4, warehouseId);

        // Verify that a new physical partition was added
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        // Clean up
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    /**
     * Test adding physical partition to a partitioned table with random distribution.
     * This tests the scenario where:
     * - Table uses RANGE partitioning with random distribution
     * - A specific partition name is provided
     */
    @Test
    public void testAddPhysicalPartitionForPartitionedTable() throws Exception {
        String tableName = "test_partitioned_random";
        String dropSQL = "drop table if exists " + tableName;
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

        // Create a partitioned table with random distribution
        String createSQL = "CREATE TABLE test_db." + tableName + " (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    v1 VARCHAR(100)\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),\n" +
                "    PARTITION p20240102 VALUES LESS THAN ('2024-01-03')\n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_db");
        OlapTable table = (OlapTable) db.getTable(tableName);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        Partition partition = table.getPartition("p20240101");
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(1, partition.getSubPartitions().size());

        // Add physical partition to a specific partition
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPhysicalPartition(db, table, "p20240101", 0, warehouseId);

        // Verify that a new physical partition was added to the specified partition
        Assertions.assertEquals(3, table.getPhysicalPartitions().size());
        Assertions.assertEquals(2, partition.getSubPartitions().size());

        // Clean up
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    /**
     * Test error handling when partition name is required but not provided for partitioned table.
     */
    @Test
    public void testAddPhysicalPartitionWithoutPartitionNameForPartitionedTable() throws Exception {
        String tableName = "test_partitioned_no_name";
        String dropSQL = "drop table if exists " + tableName;
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

        // Create a partitioned table with random distribution
        String createSQL = "CREATE TABLE test_db." + tableName + " (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    v1 VARCHAR(100)\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02')\n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_db");
        OlapTable table = (OlapTable) db.getTable(tableName);

        // Attempt to add physical partition without specifying partition name should fail
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPhysicalPartition(db, table, null, 0, warehouseId);
        });
        Assertions.assertTrue(exception.getMessage().contains("Partition name must be specified"));

        // Clean up
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    /**
     * Test error handling when specified partition does not exist.
     */
    @Test
    public void testAddPhysicalPartitionWithNonExistentPartition() throws Exception {
        String tableName = "test_nonexistent_partition";
        String dropSQL = "drop table if exists " + tableName;
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

        // Create a partitioned table with random distribution
        String createSQL = "CREATE TABLE test_db." + tableName + " (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    v1 VARCHAR(100)\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02')\n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_db");
        OlapTable table = (OlapTable) db.getTable(tableName);

        // Attempt to add physical partition for non-existent partition should fail
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPhysicalPartition(db, table, "non_existent_partition", 0, warehouseId);
        });
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));

        // Clean up
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    /**
     * Test error handling when table uses hash distribution instead of random distribution.
     */
    @Test
    public void testAddPhysicalPartitionForHashDistributionTable() throws Exception {
        String tableName = "test_hash_distribution";
        String dropSQL = "drop table if exists " + tableName;
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

        // Create a table with hash distribution
        String createSQL = "CREATE TABLE test_db." + tableName + " (\n" +
                "    k1 INT,\n" +
                "    k2 VARCHAR(50),\n" +
                "    v1 INT\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 8\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_db");
        OlapTable table = (OlapTable) db.getTable(tableName);

        // Attempt to add physical partition for hash distribution table should fail
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPhysicalPartition(db, table, null, 0, warehouseId);
        });
        Assertions.assertTrue(exception.getMessage().contains("random distribution"));

        // Clean up
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }
}
