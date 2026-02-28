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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.Map;

public interface PaimonCatalog {

    List<String> listDbNames();

    void createDb(String dbName, Map<String, String> properties) throws DdlException;

    void dropDb(String dbName, boolean isForceDrop) throws DdlException;

    List<String> listTableNames(String dbName);

    boolean createTable(CreateTableStmt stmt) throws DdlException;

    void dropTable(DropTableStmt stmt) throws DdlException;

    void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException;

    Database getDb(String dbName);

    Table getTable(String dbName, String tblName);

    long getTableUpdateTime(PaimonTable table);

    boolean tableExists(String dbName, String tableName);

    Map<String, Partition> getPartitions(String databaseName, String tableName);

    void refreshTable(String srDbName, Table table, List<String> partitionNames);

    void traceCatalogMetrics(String prefix, PaimonMetricRegistry metricRegistry);

    Catalog getNativeCatalog();

    List<Split> getSplits(Table table, Map<String, Partition> partitions,
                          Integer totalPartitionCount, long snapshotId, InnerTableScan scan);
}


