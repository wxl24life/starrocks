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

package com.starrocks.load.loadv2.etl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.SparkDppException;
import com.starrocks.load.loadv2.dpp.GlobalDictBuilder;
import com.starrocks.load.loadv2.dpp.SparkDpp;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlColumnMapping;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlTable;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SparkEtlJob is responsible for global dict building, data partition, data sort and data aggregation.
 * 1. init job config
 * 2. check if job has bitmap_dict function columns
 * 3. build global dict if step 2 is true
 * 4. dpp (data partition, data sort and data aggregation)
 */
public class SparkEtlJob {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJob.class);

    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";
    private static final String BITMAP_HASH = "bitmap_hash";

    private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private Set<Long> hiveSourceTables;
    private Map<Long, Set<String>> tableToBitmapDictColumns;
    private final SparkConf conf;
    private SparkSession spark;

    private SparkEtlJob(String jobConfigFilePath) {
        this.jobConfigFilePath = jobConfigFilePath;
        this.etlJobConfig = null;
        this.hiveSourceTables = Sets.newHashSet();
        this.tableToBitmapDictColumns = Maps.newHashMap();
        this.conf = new SparkConf();

        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "com.starrocks.load.loadv2.dpp.StarRocksKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "false");
    }

    private void initSparkEnvironment() {
        if (etlJobConfig == null || etlJobConfig.tables == null || etlJobConfig.tables.isEmpty()) {
            // todo: complete
            throw new IllegalArgumentException();
        }

        EtlTable table = etlJobConfig.tables.values().iterator().next();
        if (table == null || table.fileGroups == null || table.fileGroups.isEmpty()) {
            // todo: complete
            throw new IllegalArgumentException();
        }

        Map<String, String> properties = table.fileGroups.get(0).hiveTableProperties;

        SparkSession.Builder builder = SparkSession.builder().config(conf);
        if ("false".equals(properties.getOrDefault("maxcompute.enable", "false"))) {
            builder = builder.enableHiveSupport();
        }

        spark = builder.getOrCreate();
    }

    private void initSparkConfigs(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            spark.sparkContext().conf().set(entry.getKey(), entry.getValue());
        }
    }

    private void initConfig() {
        LOG.info("job config file path: " + jobConfigFilePath);
        String jsonConfig = getJobConfigFileContents();
        LOG.info("rdd read json config: " + jsonConfig);
        etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
        LOG.info("etl job config: " + etlJobConfig);
    }

    private String getJobConfigFileContents() {
        String jobConfigFileContents = null;

        Configuration hadoopConfig = SparkHadoopUtil.get().newConfiguration(conf);
        try (FileSystem fs = FileSystem.get(URI.create(jobConfigFilePath), hadoopConfig)) {
            Path jobConfigFile = new Path(jobConfigFilePath);
            FileStatus jobConfigFileStatus = fs.getFileStatus(jobConfigFile);
            long jobConfigFileLength = jobConfigFileStatus.getLen();
            if (jobConfigFileLength > Integer.MAX_VALUE) {
                // todo: complete
                throw new IllegalArgumentException("");
            }

            FSDataInputStream fis = fs.open(jobConfigFile);

            int offset = 0;
            int length = (int) jobConfigFileLength;
            byte[] jobConfigFileContentBytes = new byte[length];
            do {
                int readBytes = fis.read(jobConfigFileContentBytes, offset, length - offset);
                if (readBytes == -1) {
                    break;
                }
                offset += readBytes;
            } while (offset < length);

            if (offset != length) {
                // todo: complete
                throw new IOException("");
            }

            jobConfigFileContents = new String(jobConfigFileContentBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return jobConfigFileContents;
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfig() throws Exception {
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            boolean isHiveSource = false;
            Set<String> bitmapDictColumns = Sets.newHashSet();
            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                if (fileGroup.sourceType == EtlJobConfig.SourceType.HIVE) {
                    isHiveSource = true;
                }
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();
                    if (funcName.equalsIgnoreCase(BITMAP_HASH)) {
                        throw new SparkDppException("spark load not support bitmap_hash now");
                    }
                    if (funcName.equalsIgnoreCase(BITMAP_DICT_FUNC)) {
                        bitmapDictColumns.add(columnName.toLowerCase());
                    } else if (!funcName.equalsIgnoreCase(TO_BITMAP_FUNC)) {
                        newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                    }
                }
                // reset new columnMappings
                fileGroup.columnMappings = newColumnMappings;
            }
            if (isHiveSource) {
                hiveSourceTables.add(entry.getKey());
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
        }
        LOG.info("init hiveSourceTables: " + hiveSourceTables + ", tableToBitmapDictColumns: " +
                tableToBitmapDictColumns);

        // spark etl must have only one table with bitmap type column to process.
        if (hiveSourceTables.size() > 1 || tableToBitmapDictColumns.size() > 1) {
            throw new Exception("spark etl job must have only one hive table with bitmap type column to process");
        }
    }

    private void processDpp() throws Exception {
        SparkDpp sparkDpp = new SparkDpp(spark, etlJobConfig, tableToBitmapDictColumns);
        sparkDpp.init();
        sparkDpp.doDpp();
    }

    private String buildGlobalDictAndEncodeSourceTable(
            EtlTable table, Map<String, String> hiveTableProperties, long tableId) {
        // dict column map
        MultiValueMap dictColumnMap = new MultiValueMap();
        for (String dictColumn : tableToBitmapDictColumns.get(tableId)) {
            dictColumnMap.put(dictColumn, null);
        }

        // hive db and tables
        EtlFileGroup fileGroup = table.fileGroups.get(0);
        List<String> intermediateTableColumnList = fileGroup.fileFieldNames;
        String sourceHiveDBTableName = fileGroup.hiveDbTableName;
        String starrocksHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String dorisGlobalDictTableName = String.format(EtlJobConfig.DORIS_GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId, taskId);
        String starrocksIntermediateHiveTable =
                String.format(EtlJobConfig.STARROCKS_INTERMEDIATE_HIVE_TABLE_NAME, tableId, taskId);
        String sourceHiveFilter = fileGroup.where;

        LOG.info("global dict builder args, dictColumnMap: " + dictColumnMap
                + ", intermediateTableColumnList: " + intermediateTableColumnList
                + ", sourceHiveDBTableName: " + sourceHiveDBTableName
                + ", sourceHiveFilter: " + sourceHiveFilter
                + ", distinctKeyTableName: " + distinctKeyTableName
                + ", globalDictTableName: " + globalDictTableName
                + ", starrocksIntermediateHiveTable: " + starrocksIntermediateHiveTable);
        try {
            GlobalDictBuilder globalDictBuilder = new GlobalDictBuilder(
                    dictColumnMap,
                    intermediateTableColumnList,
                    sourceHiveDBTableName,
                    sourceHiveFilter,
                    starrocksHiveDB,
                    distinctKeyTableName,
                    globalDictTableName,
                    starrocksIntermediateHiveTable,
                    spark,
                    hiveTableProperties);
            globalDictBuilder.checkGlobalDictTableName(dorisGlobalDictTableName);
            globalDictBuilder.createHiveIntermediateTable();
            globalDictBuilder.extractDistinctColumn();
            globalDictBuilder.buildGlobalDict();
            globalDictBuilder.encodeStarRocksIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return String.format("%s.%s", starrocksHiveDB, starrocksIntermediateHiveTable);
    }

    private void processData() throws Exception {
        if (!hiveSourceTables.isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            Optional<Map.Entry<Long, EtlTable>> optionalEntry = etlJobConfig.tables.entrySet().stream().findFirst();
            if (optionalEntry.isPresent()) {
                Map.Entry<Long, EtlTable> entry = optionalEntry.get();
                tableId = entry.getKey();
                table = entry.getValue();
            }

            if (table == null) {
                throw new SparkDppException("invalid etl job config");
            }
            // init hive configs like metastore service
            EtlFileGroup fileGroup = table.fileGroups.get(0);
            initSparkConfigs(fileGroup.hiveTableProperties);
            fileGroup.dppHiveDbTableName = fileGroup.hiveDbTableName;

            // build global dict and encode source hive table if has bitmap dict columns
            if (!tableToBitmapDictColumns.isEmpty() && tableToBitmapDictColumns.containsKey(tableId)) {
                String starrocksIntermediateHiveDbTableName = buildGlobalDictAndEncodeSourceTable(
                        table, fileGroup.hiveTableProperties, tableId);
                // set with starrocksIntermediateHiveDbTable
                fileGroup.dppHiveDbTableName = starrocksIntermediateHiveDbTableName;
            }
        }

        // data partition sort and aggregation
        processDpp();
    }

    private void run() throws Exception {
        try {
            // Initialize config before initializing spark environment for creating suitable SparkSession.
            initConfig();
            initSparkEnvironment();
            checkConfig();
            processData();
        } finally {
            // SparkSession.close will close some thread in order to avoid hanging.
            if (spark != null) {
                spark.close();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("missing job config file path arg");
            System.exit(-1);
        }

        try {
            new SparkEtlJob(args[0]).run();
        } catch (Exception e) {
            System.err.println("spark etl job run failed");
            LOG.warn(e);
            System.exit(-1);
        }
    }
}
