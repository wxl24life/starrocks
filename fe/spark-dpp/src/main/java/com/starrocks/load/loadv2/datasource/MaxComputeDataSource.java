// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.loadv2.datasource;

import com.google.common.collect.Maps;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

public class MaxComputeDataSource extends DataSource {

    private static final String LEGACY_PASS_PARTITION_BY_AS_OPTIONS =
            "spark.sql.legacy.sources.write.passPartitionByAsOptions";

    public MaxComputeDataSource(SparkSession spark, String format, Map<String, String> dataSourceOptions) {
        super(spark, format, dataSourceOptions);
        this.spark.conf().set(LEGACY_PASS_PARTITION_BY_AS_OPTIONS, "true");
    }

    @Override
    protected Map<String, String> getDataSourceOptions(String fullTableName) {
        String[] databaseAndTable = fullTableName.split("\\.");
        if (databaseAndTable.length != 2) {
            throw new IllegalArgumentException(String.format(
                    "Invalid table name %s, should be specified as database.table", fullTableName));
        }
        Map<String, String> copiedOptions = Maps.newHashMap(dataSourceOptions);
        copiedOptions.put("project", databaseAndTable[0]);
        copiedOptions.put("table", databaseAndTable[1]);
        return copiedOptions;
    }

    @Override
    protected Dataset<Row> readTable(String fullTableName, String format, Map<String, String> options) {
        return spark.read().format(format).options(options).load();
    }

    @Override
    protected void writeData(
            Dataset<Row> data,
            String fullTableName,
            String format,
            SaveMode saveMode,
            List<String> partitionBy,
            Map<String, String> options) {
        DataFrameWriter<Row> writer = data.write();
        if (partitionBy != null && !partitionBy.isEmpty()) {
            writer = writer.partitionBy(JavaConverters.asScalaBufferConverter(partitionBy).asScala());
        }
        writer.format(format).mode(saveMode).options(options).save();
    }
}
