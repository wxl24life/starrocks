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

package com.starrocks.lakeoptimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.common.Config;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class LakeOptimizerMetaManager extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(LakeOptimizerMetaManager.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";

    public LakeOptimizerMetaManager() {
        super("lake optimizer meta manager", 30L * 1000L);
    }

    private boolean checkDatabaseExist() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME) != null;
    }

    private boolean createDatabase() {
        LOG.info("{} Creating database", LOG_PREFIX);
        CreateDbStmt dbStmt = new CreateDbStmt(false, LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(dbStmt.getFullDbName());
        } catch (StarRocksException e) {
            LOG.warn("{} Failed to create database", LOG_PREFIX, e);
            return false;
        }
        LOG.info("{} Database created", LOG_PREFIX);
        return checkDatabaseExist();
    }

    private boolean checkTableExist(String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME);
        Preconditions.checkState(db != null);
        return db.getTable(tableName) != null;
    }

    private int calBucketNumber() throws StarRocksException {
        if (RunMode.isSharedDataMode()) {
            // Use compute node count in default warehouse in shared-data mode
            java.util.List<Long> computeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getAllComputeNodeIds(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            int bucketNumber = computeNodeIds.size();
            if (bucketNumber == 0) {
                throw new NoAliveBackendException("No compute nodes in default warehouse");
            }
            return bucketNumber;
        } else {
            // Use backend count in shared-nothing mode
            int backendNumber = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendIds(false).size();
            if (backendNumber == 0) {
                throw new NoAliveBackendException("No alive backend");
            }
            return backendNumber;
        }
    }

    private boolean createSchemaTable(ConnectContext context) {
        LOG.info("{} Creating table_schema table as PRIMARY KEY table", LOG_PREFIX);
        TableName tableName = new TableName(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            // Primary key: (catalog_name, database_name, table_name)
            List<String> primaryKeyColumns = ImmutableList.of("catalog_name", "database_name", "table_name");
            KeysDesc keysDesc = new KeysDesc(KeysType.PRIMARY_KEYS, primaryKeyColumns);
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    LakeOptimizerUtils.buildLakeOptimizerColumnRef(LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    keysDesc,
                    null,
                    new HashDistributionDesc(1, primaryKeyColumns),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("{} Failed to create table_schema", LOG_PREFIX, e);
            return false;
        }
        LOG.info("{} table_schema created", LOG_PREFIX);
        return checkTableExist(LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME);
    }

    private boolean createPartitionTable(ConnectContext context) {
        LOG.info("{} Creating partition_statistics table", LOG_PREFIX);
        TableName tableName = new TableName(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();
        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            List<String> distributionColumns = ImmutableList.of("table_id");
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    LakeOptimizerUtils.buildLakeOptimizerColumnRef(LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    null,
                    null,
                    new HashDistributionDesc(3, distributionColumns),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("{} Failed to create partition_statistics", LOG_PREFIX, e);
            return false;
        }
        LOG.info("{} partition_statistics created", LOG_PREFIX);
        return checkTableExist(LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME);
    }

    private boolean createFileStatisticsTable(ConnectContext context) {
        LOG.info("{} Creating file_statistics table", LOG_PREFIX);
        TableName tableName = new TableName(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME,
                LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME);
        Map<String, String> properties = Maps.newHashMap();

        try {
            int defaultReplicationNum = AutoInferUtil.calDefaultReplicationNum();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Integer.toString(defaultReplicationNum));
            List<String> distributionColumns = ImmutableList.of("table_id", "partition_name");
            int bucketNumber = calBucketNumber();
            CreateTableStmt stmt = new CreateTableStmt(false, false,
                    tableName,
                    LakeOptimizerUtils.buildLakeOptimizerColumnRef(
                            LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME),
                    EngineType.defaultEngine().name(),
                    null,
                    null,
                    new HashDistributionDesc(bucketNumber, distributionColumns),
                    properties,
                    null,
                    "");

            Analyzer.analyze(stmt, context);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        } catch (StarRocksException e) {
            LOG.warn("{} Failed to create file_statistics", LOG_PREFIX, e);
            return false;
        }
        LOG.info("{} file_statistics created", LOG_PREFIX);
        return checkTableExist(LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME);
    }

    private void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    private boolean createTable(String tableName) {
        ConnectContext context = LakeOptimizerUtils.buildConnectContext();
        try (ConnectContext.ScopeGuard guard = context.bindScope()) {
            if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME)) {
                return createSchemaTable(context);
            } else if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME)) {
                return createPartitionTable(context);
            } else if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME)) {
                return createFileStatisticsTable(context);
            } else {
                throw new StarRocksPlannerException("Error table name " + tableName, ErrorType.INTERNAL_ERROR);
            }
        }
    }

    private void refreshLakeOptimizerTable(String tableName) {
        while (!checkTableExist(tableName)) {
            if (createTable(tableName)) {
                break;
            }
            LOG.warn("{} Failed to create table {}", LOG_PREFIX, tableName);
            trySleep(10000);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_lake_optimizer) {
            return;
        }
        while (!checkDatabaseExist()) {
            if (createDatabase()) {
                break;
            }
            trySleep(10000);
        }
        refreshLakeOptimizerTable(LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME);
        refreshLakeOptimizerTable(LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME);
        refreshLakeOptimizerTable(LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME);
    }
}
