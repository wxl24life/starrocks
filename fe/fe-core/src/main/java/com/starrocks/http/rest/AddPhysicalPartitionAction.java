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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

/**
 * HTTP API to add a physical partition to a table.
 * This API is primarily used for cross-cluster data replication scenarios.
 *
 * Usage:
 *   POST /api/{db}/{table}/_add_physical_partition?partition={partition_name}&buckets={bucket_num}
 *
 * Parameters:
 *   - db: database name (required, in path)
 *   - table: table name (required, in path)
 *   - partition: partition name (optional, required for partitioned tables)
 *   - buckets: bucket number (optional, default is system auto-infer)
 *
 * Example:
 *   curl -X POST -u root: "http://fe_host:http_port/api/test_db/test_table/_add_physical_partition?partition=p1&buckets=16"
 */
public class AddPhysicalPartitionAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(AddPhysicalPartitionAction.class);

    private static final String PARTITION_KEY = "partition";
    private static final String BUCKETS_KEY = "buckets";

    public AddPhysicalPartitionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_add_physical_partition",
                new AddPhysicalPartitionAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        // Redirect to leader if necessary
        if (redirectToLeader(request, response)) {
            return;
        }

        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);

        if (Strings.isNullOrEmpty(dbName)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Missing database name");
        }
        if (Strings.isNullOrEmpty(tableName)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Missing table name");
        }

        // Check ALTER privilege on the table
        Authorizer.checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.ALTER);

        String partitionName = request.getSingleParameter(PARTITION_KEY);
        String bucketsStr = request.getSingleParameter(BUCKETS_KEY);
        int bucketNum = 0;
        if (!Strings.isNullOrEmpty(bucketsStr)) {
            try {
                bucketNum = Integer.parseInt(bucketsStr);
                if (bucketNum < 0) {
                    throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                            "Bucket number must be non-negative");
                }
                if (bucketNum > Config.max_bucket_number_per_partition) {
                    throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                            "Bucket number exceeds maximum allowed: " + Config.max_bucket_number_per_partition);
                }
            } catch (NumberFormatException e) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Invalid bucket number: " + bucketsStr);
            }
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                    "Database '" + dbName + "' does not exist");
        }

        // Use a single write lock block to avoid race conditions during lock upgrade.
        // Previously, releasing read lock and reacquiring write lock could allow
        // concurrent DDL to drop/replace the table, causing NPE or ClassCastException.
        try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(),
                Collections.emptyList(), LockType.WRITE)) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Table '" + tableName + "' does not exist");
            }

            if (!(table instanceof OlapTable)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Only OLAP table supports adding physical partition");
            }

            OlapTable olapTable = (OlapTable) table;

            // Check if the table uses random distribution
            if (olapTable.getDefaultDistributionInfo().getType() != DistributionInfo.DistributionInfoType.RANDOM) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Only random distribution table supports adding physical partition");
            }

            // Validate partition name for partitioned tables
            if (partitionName == null && olapTable.getPartitionInfo().isPartitioned()) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "Partition name must be specified for partitioned table");
            }

            long warehouseId = ConnectContext.get().getCurrentWarehouseId();
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPhysicalPartition(db, olapTable, partitionName, bucketNum, warehouseId);
        }

        LOG.info("Successfully added physical partition via HTTP API. db: {}, table: {}, partition: {}, buckets: {}",
                dbName, tableName, partitionName, bucketNum);

        sendResult(request, response, new RestBaseResult());
    }
}
