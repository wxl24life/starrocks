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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TypeDef;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.warehouse.Warehouse;

import java.util.List;

public class LakeOptimizerUtils {
    public static ConnectContext buildConnectContext() {
        ConnectContext context = ConnectContext.buildInner();
        context.getSessionVariable().setEnableProfile(false);
        context.getSessionVariable().setEnableLoadProfile(false);
        context.getSessionVariable().setBigQueryProfileThreshold("0s");
        context.getSessionVariable().setParallelExecInstanceNum(1);
        context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
        context.getSessionVariable().setEnablePipelineEngine(true);
        context.getSessionVariable().setCboCteReuse(true);
        context.getSessionVariable().setCboCTERuseRatio(0);

        WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = manager.getBackgroundWarehouse();
        context.getSessionVariable().setWarehouseName(warehouse.getName());

        context.setDatabase(LakeOptimizerConstants.LAKE_OPTIMIZER_DB_NAME);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setStartTime();

        return context;
    }

    public static List<ColumnDef> buildLakeOptimizerColumnRef(String tableName) {
        ScalarType databaseNameType = ScalarType.createVarcharType(65530);
        ScalarType tableNameType = ScalarType.createVarcharType(65530);
        ScalarType partitionNameType = ScalarType.createVarcharType(65530);
        ScalarType catalogNameType = ScalarType.createVarcharType(65530);

        ScalarType partitionValuesType = ScalarType.createVarbinary(65530);
        ScalarType fileNameType = ScalarType.createVarcharType(65530);

        ScalarType minMaxType = ScalarType.createVarbinary(65530);
        ScalarType nullCountType = ScalarType.createVarbinary(65530);

        if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_SCHEMA_TABLE_NAME)) {
            // Primary key: (catalog_name, database_name, table_name)
            return ImmutableList.of(
                    new ColumnDef("catalog_name", new TypeDef(catalogNameType)),
                    new ColumnDef("database_name", new TypeDef(databaseNameType)),
                    new ColumnDef("table_name", new TypeDef(tableNameType)),
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("table_uuid", new TypeDef(ScalarType.createVarcharType(256))),
                    new ColumnDef("begin_snapshot", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("end_snapshot", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("bucket_num", new TypeDef(ScalarType.createType(PrimitiveType.INT)))
            );
        } else if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_PARTITIONS_TABLE_NAME)) {
            return ImmutableList.of(
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("partition_name", new TypeDef(partitionNameType)),
                    new ColumnDef("snapshot_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("partition_values", new TypeDef(partitionValuesType)),
                    new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("data_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("file_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("min_key", new TypeDef(minMaxType), true),
                    new ColumnDef("max_key", new TypeDef(minMaxType), true),
                    new ColumnDef("last_file_creation_time", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)))
            );
        } else if (tableName.equals(LakeOptimizerConstants.LAKE_OPTIMIZER_FILE_STATISTICS_TABLE_NAME)) {
            ScalarType deletionPathType = ScalarType.createVarcharType(65530);
            ScalarType externalPathType = ScalarType.createVarcharType(65530);
            ScalarType embeddedIndexType = ScalarType.createVarbinary(65530);
            return ImmutableList.of(
                    // Lake Optimizer specific fields (NOT NULL)
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("partition_name", new TypeDef(partitionNameType)),
                    new ColumnDef("bucket", new TypeDef(ScalarType.createType(PrimitiveType.INT))),
                    new ColumnDef("snapshot_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("total_buckets", new TypeDef(ScalarType.createType(PrimitiveType.INT))),
                    new ColumnDef("file_kind", new TypeDef(ScalarType.createType(PrimitiveType.TINYINT))),
                    // DataFileMeta fields - NOT NULL
                    new ColumnDef("file_name", new TypeDef(fileNameType)),
                    new ColumnDef("file_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("min_key", new TypeDef(minMaxType)),
                    new ColumnDef("max_key", new TypeDef(minMaxType)),
                    new ColumnDef("key_stats_min", new TypeDef(minMaxType)),
                    new ColumnDef("key_stats_max", new TypeDef(minMaxType)),
                    new ColumnDef("key_stats_null_count", new TypeDef(nullCountType)),
                    new ColumnDef("value_stats_min", new TypeDef(minMaxType)),
                    new ColumnDef("value_stats_max", new TypeDef(minMaxType)),
                    new ColumnDef("value_stats_null_count", new TypeDef(nullCountType)),
                    new ColumnDef("min_sequence_number", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("max_sequence_number", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("schema_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("level", new TypeDef(ScalarType.createType(PrimitiveType.INT))),
                    new ColumnDef("extra_files", new TypeDef(new ArrayType(ScalarType.createVarcharType(65530)))),
                    new ColumnDef("creation_time", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    // DataFileMeta fields - NULLABLE
                    new ColumnDef("delete_row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true),
                    new ColumnDef("embedded_file_index", new TypeDef(embeddedIndexType), true),
                    new ColumnDef("file_source", new TypeDef(ScalarType.createType(PrimitiveType.TINYINT)), true),
                    new ColumnDef("value_stats_cols", new TypeDef(new ArrayType(ScalarType.createVarcharType(65530))), true),
                    new ColumnDef("external_path", new TypeDef(externalPathType), true),
                    new ColumnDef("first_row_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true),
                    new ColumnDef("write_cols", new TypeDef(new ArrayType(ScalarType.createVarcharType(65530))), true),
                    // Deletion file fields for DeletionVector support (NULLABLE)
                    new ColumnDef("deletion_path", new TypeDef(deletionPathType), true),
                    new ColumnDef("deletion_offset", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true),
                    new ColumnDef("deletion_length", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true),
                    new ColumnDef("deletion_cardinality", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true)
            );
        } else {
            throw new StarRocksPlannerException("Not support stats table " + tableName, ErrorType.INTERNAL_ERROR);
        }
    }
}
