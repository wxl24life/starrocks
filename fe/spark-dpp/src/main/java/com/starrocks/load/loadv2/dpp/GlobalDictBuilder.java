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

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.datasource.DataSource;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * used for build hive global dict and encode source hive table
 * <p>
 * input: a source hive table
 * output: a intermediate hive table whose distinct column is encode with int value
 * <p>
 * usage example
 * step1,create a intermediate hive table
 * GlobalDictBuilder.createHiveIntermediateTable()
 * step2, get distinct column's value
 * GlobalDictBuilder.extractDistinctColumn()
 * step3, build global dict
 * GlobalDictBuilder.buildGlobalDict()
 * step4, encode intermediate hive table with global dict
 * GlobalDictBuilder.encodeStarRocksIntermediateHiveTable()
 */

public class GlobalDictBuilder {

    protected static final Logger LOG = LogManager.getLogger(GlobalDictBuilder.class);

    // name of the column in starrocks table which need to build global dict
    // for example: some dict columns a,b,c
    // case 1: all dict columns has no relation, then the map is as below
    //     [a=null, b=null, c=null]
    // case 2: column a's value can reuse column b's value which means column a's value is a subset of column b's value
    //  [b=a,c=null]
    private final MultiValueMap dictColumn;
    // intermediate table columns in current spark load job
    private final List<String> intermediateTableColumnList;

    // source hive database name
    private final String starrocksHiveDB;

    // hive table datasource,format is db.table
    private final String sourceHiveDBTableName;
    // user-specified filter when query sourceHiveDBTable
    private final String sourceHiveFilter;
    // intermediate hive table to store the distinct value of distinct column
    private final String distinctKeyTableName;
    // current starrocks table's global dict hive table
    private String globalDictTableName;

    // used for next step to read
    private final String starrocksIntermediateHiveTable;
    private final SparkSession spark;

    // key=starrocks column name,value=column type
    private final Map<String, String> starrocksColumnNameTypeMap = new HashMap<>();

    private final DataSource dataSource;

    public GlobalDictBuilder(MultiValueMap dictColumn,
                             List<String> intermediateTableColumnList,
                             String sourceHiveDBTableName,
                             String sourceHiveFilter,
                             String starrocksHiveDB,
                             String distinctKeyTableName,
                             String globalDictTableName,
                             String starrocksIntermediateHiveTable,
                             SparkSession spark,
                             Map<String, String> dataSourceOptions) {
        this.dictColumn = dictColumn;
        this.intermediateTableColumnList = intermediateTableColumnList;
        this.sourceHiveDBTableName = sourceHiveDBTableName;
        this.sourceHiveFilter = sourceHiveFilter;
        this.starrocksHiveDB = starrocksHiveDB;
        this.distinctKeyTableName = distinctKeyTableName;
        this.globalDictTableName = globalDictTableName;
        this.starrocksIntermediateHiveTable = starrocksIntermediateHiveTable;
        this.spark = spark;
        this.dataSource = DataSource.fromProperties(spark, dataSourceOptions);

        LOG.info("use " + starrocksHiveDB);
    }

    /**
     * Check if doris global dict table already exist.
     * If exists, use old name for compatibility.
     *
     * @param dorisGlobalDictTableName old global dict table name in previous version
     */
    public void checkGlobalDictTableName(String dorisGlobalDictTableName) {
        try {
            dataSource.getOrLoadTable(starrocksHiveDB, dorisGlobalDictTableName);
            globalDictTableName = dorisGlobalDictTableName;
        } catch (Exception e) {
            // should do nothing.
        }
        LOG.info("global dict table name: " + globalDictTableName);
    }

    public void createHiveIntermediateTable() {
        Dataset<Row> sourceHiveDBTable = dataSource.getOrLoadTable(sourceHiveDBTableName);

        Map<String, String> sourceHiveTableColumn = Arrays.stream(sourceHiveDBTable.schema().fields())
                .map(structField -> new ImmutablePair<>(structField.name().toLowerCase(), structField.dataType().typeName()))
                .collect(Collectors.toMap(ImmutablePair<String, String>::getLeft, ImmutablePair<String, String>::getRight));

        // check and get starrocks column type in hive
        intermediateTableColumnList.stream().map(String::toLowerCase).forEach(columnName -> {
            String columnType = sourceHiveTableColumn.get(columnName);
            if (StringUtils.isEmpty(columnType)) {
                throw new RuntimeException(String.format("starrocks column %s not in source hive table", columnName));
            }
            starrocksColumnNameTypeMap.put(columnName, columnType.toLowerCase());
        });

        Set<String> allDictColumn = new HashSet<>();
        allDictColumn.addAll(dictColumn.keySet());
        allDictColumn.addAll(dictColumn.values());

        List<String> targetColumns = intermediateTableColumnList.stream().map(columnName -> {
            if (allDictColumn.contains(columnName)) {
                return "cast(" + columnName + " as string) as " + columnName;
            } else {
                return "cast(" + columnName + " as " + starrocksColumnNameTypeMap.get(columnName) + ") as " + columnName;
            }
        }).collect(Collectors.toList());

        Dataset<Row> output = sourceHiveDBTable.selectExpr(
                JavaConverters.collectionAsScalaIterableConverter(targetColumns).asScala().toSeq());
        if (!StringUtils.isEmpty(sourceHiveFilter)) {
            output = output.where(sourceHiveFilter);
        }
        dataSource.writeTable(output, SaveMode.Overwrite, null, starrocksHiveDB, starrocksIntermediateHiveTable);
    }

    public void extractDistinctColumn() {
        Dataset<Row> intermediateHiveTable = dataSource.getOrLoadTable(starrocksHiveDB, starrocksIntermediateHiveTable);

        // extract distinct column
        // For the column in dictColumn's valueSet, their value is a subset of column in keyset,
        // so we don't need to extract distinct value of column in valueSet
        for (Object column : dictColumn.keySet()) {
            String columnName = column.toString();
            Dataset<Row> distinctColumn = intermediateHiveTable
                    .selectExpr(columnName, "'" + columnName + "' as dict_column")
                    .distinct()
                    .toDF("dict_key", "dict_column");

            String partitionSpec = "dict_column=" + column;
            dataSource.writeTable(
                    distinctColumn, SaveMode.Overwrite, partitionSpec, starrocksHiveDB, distinctKeyTableName);
        }
    }

    public void buildGlobalDict() {
        createGlobalDictTable();

        for (Object distinctColumnNameOrigin : dictColumn.keySet()) {
            final String distinctColumnName = distinctColumnNameOrigin.toString();
            final String filterDistinctColumn = "dict_column='" + distinctColumnName + "'";

            List<Row> maxGlobalDictValueRow = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                    .where(filterDistinctColumn)
                    .selectExpr("max(dict_value) as max_value", "min(dict_value) as min_value")
                    .collectAsList();
            if (maxGlobalDictValueRow.size() == 0) {
                throw new RuntimeException(String.format("get max dict value failed: %s", distinctColumnName));
            }

            long maxDictValue = 0;
            long minDictValue = 0;
            Row row = maxGlobalDictValueRow.get(0);
            if (row != null && row.get(0) != null) {
                maxDictValue = (long) row.get(0);
                minDictValue = (long) row.get(1);
            }
            LOG.info("column " + distinctColumnName + " 's max value in dict is " + maxDictValue +
                    ", min value is " + minDictValue);

            // maybe never happened, but we need detect it
            if (minDictValue < 0) {
                throw new RuntimeException(String.format(" column %s 's cardinality has exceed bigint's max value",
                        distinctColumnName));
            }

            Dataset<Row> source = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                    .where(filterDistinctColumn)
                    .select("dict_key", "dict_value");

            Dataset<Row> t1 = dataSource.getOrLoadTable(starrocksHiveDB, distinctKeyTableName)
                    .where(filterDistinctColumn)
                    .where("dict_key is not null")
                    .select("dict_key")
                    .alias("t1");

            Dataset<Row> t2 = source.alias("t2");

            Dataset<Row> joinedTable = t1
                    .join(t2, t1.col("dict_key").equalTo(t2.col("dict_key")), "leftouter")
                    .where(t2.col("dict_value").isNull())
                    .selectExpr(
                            "t1.dict_key as dict_key",
                            "(row_number() over(order by t1.dict_key)) + (" + maxDictValue + ") as dict_value");

            Dataset<Row> result = source.unionAll(joinedTable)
                    .selectExpr("dict_key", "dict_value", "'" + distinctColumnName + "' as dict_column");

            String partitionSpec = "dict_column=" + distinctColumnName;

            dataSource.writeTable(result, SaveMode.Overwrite, partitionSpec, starrocksHiveDB, globalDictTableName);
        }
    }

    // encode starrocksIntermediateHiveTable's distinct column
    public void encodeStarRocksIntermediateHiveTable() {
        for (Object distinctColumnObj : dictColumn.keySet()) {
            String dictColumnName = distinctColumnObj.toString();
            List<String> childColumn = (ArrayList) dictColumn.get(dictColumnName);

            Dataset<Row> t1 = dataSource.getOrLoadTable(starrocksHiveDB, starrocksIntermediateHiveTable).alias("t1");
            StructField[] schema = t1.schema().fields();

            Dataset<Row> t2 = dataSource.getOrLoadTable(starrocksHiveDB, globalDictTableName)
                    .select("dict_key", "dict_value")
                    .where("dict_column='" + dictColumnName + "'")
                    .alias("t2");

            List<String> selectExprs = new ArrayList<>(intermediateTableColumnList.size());
            for (int idx = 0; idx < intermediateTableColumnList.size(); ++idx) {
                String columnName = intermediateTableColumnList.get(idx);
                String columnType = schema[idx].dataType().typeName();

                String expr;
                if (dictColumnName.equals(columnName)) {
                    expr = String.format("cast(t2.dict_value as %s) as %s", columnType, columnName);
                } else if (childColumn != null && childColumn.contains(columnName)) {
                    expr = String.format("cast(if(%s is null, null, t2.dict_value) as %s) as %s",
                        columnName, columnType, columnName);
                } else {
                    expr = String.format("cast(t1.%s as %s) as %s", columnName, columnType, columnName);
                }
                selectExprs.add(expr);
            }

            Dataset<Row> result = t1.join(t2, t1.col(dictColumnName).equalTo(t2.col("dict_key")), "leftouter")
                    .selectExpr(JavaConverters.asScalaBufferConverter(selectExprs).asScala());

            // use tmp table to avoid "overwrite table that is also being read from"
            dataSource.writeTable(result, SaveMode.Overwrite, null, starrocksHiveDB, starrocksIntermediateHiveTable);
        }
    }

    private void createGlobalDictTable() {
        // use empty data frame to create table.
        StructType type = new StructType()
                .add("dict_key", StringType)
                .add("dict_value", LongType)
                .add("dict_column", StringType);
        Dataset<Row> empty = spark.createDataFrame(new ArrayList<>(), type);

        StringBuilder partitionSpec = new StringBuilder();
        for (Object distinctColumnNameOrigin : dictColumn.keySet()) {
            final String distinctColumnName = distinctColumnNameOrigin.toString();
            partitionSpec.append(",").append("dict_column=").append(distinctColumnName);
        }
        dataSource.writeTable(empty, SaveMode.Ignore, partitionSpec.deleteCharAt(0).toString(),
                starrocksHiveDB, globalDictTableName);
    }

}
