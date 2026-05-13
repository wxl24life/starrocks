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

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.odps.EntityConvertUtils;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class OdpsTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OdpsTable.class);
    public static final String PARTITION_NULL_VALUE = "null";

    @SerializedName(value = "tn")
    private String tableName;

    private Map<String, String> properties;
    private String catalogName;
    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "pn")
    private String projectName;
    @SerializedName(value = "sn")
    private String schemaName;
    private List<Column> dataColumns;
    private List<Column> partitionColumns;

    public OdpsTable() {
        super(TableType.ODPS);
    }

    public OdpsTable(String catalogName, com.aliyun.odps.Table odpsTable) {
        this(catalogName, odpsTable.getProject(), null, odpsTable);
    }

    public OdpsTable(String catalogName, String projectName, String schemaName, com.aliyun.odps.Table odpsTable) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), odpsTable.getName(), TableType.ODPS,
                EntityConvertUtils.getFullSchema(odpsTable));
        this.createTime = odpsTable.getCreatedTime().getTime();
        this.catalogName = catalogName;
        this.projectName = projectName;
        this.schemaName = schemaName;
        this.dbName = (schemaName != null) ? schemaName : projectName;
        this.tableName = odpsTable.getName();
        this.partitionColumns =
                odpsTable.getSchema().getPartitionColumns().stream().map(EntityConvertUtils::convertColumn).collect(
                        Collectors.toList());
        this.dataColumns = odpsTable.getSchema().getColumns().stream().map(EntityConvertUtils::convertColumn).collect(
                Collectors.toList());
    }

    public String getProjectName() {
        return projectName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getResourceName() {
        return tableName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    @Override
    public List<String> getDataColumnNames() {
        return dataColumns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
    }

    @Override
    public boolean isUnPartitioned() {
        return partitionColumns.isEmpty();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    public String getUUID() {
        if (schemaName != null) {
            return String.join(".", catalogName, projectName, schemaName, name, Long.toString(createTime));
        }
        // Legacy two-tier mode: keep UUID format unchanged (catalog.project.name.createTime).
        // Fall back to dbName when projectName is absent (e.g., on deserialized old objects).
        String project = projectName != null ? projectName : dbName;
        return String.join(".", catalogName, project, name, Long.toString(createTime));
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        // Always pass the real MaxCompute project name in the dbName slot so BE/scanner
        // sees the project (not schema) as `project_name` — preserves the legacy contract.
        String beDbName = projectName != null ? projectName : getCatalogDBName();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ODPS_TABLE,
                fullSchema.size(), 0, getName(), beDbName);
        THdfsTable hdfsTable = new THdfsTable();
        hdfsTable.setColumns(getColumns().stream().map(Column::toThrift).collect(Collectors.toList()));
        // for be, partition column is equals to data column
        hdfsTable.setPartition_columnsIsSet(false);
        hdfsTable.setTime_zone(TimeUtils.getSessionTimeZone());
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
