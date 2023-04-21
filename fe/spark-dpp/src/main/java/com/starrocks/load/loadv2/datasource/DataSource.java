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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class DataSource {

    public static DataSource fromProperties(SparkSession spark, Map<String, String> properties) {
        String format = properties.getOrDefault("format", "hive");
        switch (format.toLowerCase()) {
            case "odps":
            case "org.apache.spark.aliyun.odps.datasource":
                return new MaxComputeDataSource(spark, format, properties);
            case "hive":
            default:
                return new HiveDataSource(spark, format, properties);
        }
    }

    protected final SparkSession spark;
    protected final Map<String, String> dataSourceOptions;

    private final String format;
    // "database.table" => dataSourceOptions
    private final Map<String, Map<String, String>> fullTableNameToDataSourceOptions;

    public DataSource(SparkSession spark, String format, Map<String, String> dataSourceOptions) {
        this.spark = spark;
        this.format = format;
        this.dataSourceOptions = dataSourceOptions;
        this.fullTableNameToDataSourceOptions = new HashMap<>();
    }

    protected abstract Map<String, String> getDataSourceOptions(String fullTableName);

    protected abstract Dataset<Row> readTable(String fullTableName, String format, Map<String, String> options);

    protected abstract void writeData(
            Dataset<Row> data,
            String fullTableName,
            String format,
            SaveMode saveMode,
            List<String> partitionBy,
            Map<String, String> options);

    private Map<String, String> getOrCreateDataSourceOptions(String fullTableName) {
        return fullTableNameToDataSourceOptions.computeIfAbsent(fullTableName, this::getDataSourceOptions);
    }

    public Dataset<Row> getOrLoadTable(String database, String table) {
        return getOrLoadTable(database + "." + table);
    }

    public Dataset<Row> getOrLoadTable(String fullTableName) {
        return readTable(fullTableName, format, getOrCreateDataSourceOptions(fullTableName));
    }

    public void writeTable(Dataset<Row> data, SaveMode mode, String partitionSpec, String database, String table) {
        writeTable(data, mode, partitionSpec, database + "." + table);
    }

    public void writeTable(Dataset<Row> data, SaveMode mode, String partitionSpec, String fullTableName) {
        Map<String, String> options = Maps.newHashMap(getOrCreateDataSourceOptions(fullTableName));
        List<String> partitions = new ArrayList<>();
        if (partitionSpec != null && !partitionSpec.isEmpty()) {
            partitions.addAll(Arrays.stream(partitionSpec.split(",")).map(spec -> {
                String[] pair = spec.split("=");
                if (pair.length != 2) {
                    throw new IllegalArgumentException("");
                }
                return pair[0];
            }).collect(Collectors.toSet()));
            options.put("partitionSpec", partitionSpec);
        }
        writeData(data, fullTableName, format, mode, partitions, options);
    }
}
