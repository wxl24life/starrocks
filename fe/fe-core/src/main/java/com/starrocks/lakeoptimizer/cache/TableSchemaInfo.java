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

/**
 * Table schema information stored in LakeOptimizer.
 */
public class TableSchemaInfo {
    private final long tableId;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final String tableUuid;
    private final long beginSnapshot;
    private final long endSnapshot;
    private final int bucketNum;

    public TableSchemaInfo(long tableId, String catalogName, String databaseName,
                           String tableName, String tableUuid, long beginSnapshot, long endSnapshot,
                           int bucketNum) {
        this.tableId = tableId;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableUuid = tableUuid;
        this.beginSnapshot = beginSnapshot;
        this.endSnapshot = endSnapshot;
        this.bucketNum = bucketNum;
    }

    public long getTableId() {
        return tableId;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableUuid() {
        return tableUuid;
    }

    public long getBeginSnapshot() {
        return beginSnapshot;
    }

    public long getEndSnapshot() {
        return endSnapshot;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public String toString() {
        return String.format("TableSchemaInfo{tableId=%d, catalog=%s, db=%s, table=%s, " +
                        "uuid=%s, snapshots=[%d, %d), bucketNum=%d}",
                tableId, catalogName, databaseName, tableName,
                tableUuid, beginSnapshot, endSnapshot, bucketNum);
    }
}

