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

package com.starrocks.connector.index;

import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;

public class TableIndexMetadata implements ConnectorMetadata {

    public static final String INDEX_DB_NAME = "index_database";

    public static boolean isIndexTable(String tableName) {
        return tableName.toLowerCase().endsWith(IndexTable.INDEX_TABLE_SUFFIX);
    }

    @Override
    public Table getTable(com.starrocks.qe.ConnectContext context, String dbName, String tblName) {
        return new IndexTable();
    }
}
