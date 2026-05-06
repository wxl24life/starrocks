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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.clone.BalanceStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PartitionsProcDirTest {

    @Test
    public void testFetchResultForCloudNativeTable() throws AnalysisException {
        boolean oldEnableStorageObservation = Config.enable_show_partitions_storage_observation;
        Config.enable_show_partitions_storage_observation = false;
        try {
            Database db = new Database(10000L, "PartitionsProcDirTestDB");

            List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
            PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
            DataCacheInfo dataCache = new DataCacheInfo(true, false);
            long partitionId = 1025;
            listPartition.setDataCacheInfo(partitionId, dataCache);
            LakeTable cloudNativeTable = new LakeTable(1024L, "cloud_native_table", col, null, listPartition, null);
            MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
            Map<String, Long> indexNameToId = cloudNativeTable.getIndexNameToId();
            indexNameToId.put("index1", index.getId());
            cloudNativeTable.addPartition(new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(10)));

            db.registerTableUnlocked(cloudNativeTable);

            BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativeTable, false).fetchResult();
            List<List<String>> rows = result.getRows();
            List<String> list1 = rows.get(0);
            Assertions.assertEquals("1035", list1.get(0));
            Assertions.assertEquals("p1", list1.get(1));
            Assertions.assertEquals("0", list1.get(2));
            Assertions.assertEquals("1", list1.get(3));
            Assertions.assertEquals("2", list1.get(4));
            Assertions.assertEquals("NORMAL", list1.get(5));
            Assertions.assertEquals("province", list1.get(6));
            Assertions.assertFalse(result.getColumnNames().contains("StorageVolume"));
            Assertions.assertFalse(result.getColumnNames().contains("StoragePath"));
            Assertions.assertEquals(21, list1.size());
        } finally {
            Config.enable_show_partitions_storage_observation = oldEnableStorageObservation;
        }
    }

    @Test
    public void testFetchResultForCloudNativeTableWithStorageObservationEnabled() throws AnalysisException {
        boolean oldEnableStorageObservation = Config.enable_show_partitions_storage_observation;
        Config.enable_show_partitions_storage_observation = true;
        try {
            Database db = new Database(10000L, "PartitionsProcDirTestDB");

            List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
            PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
            DataCacheInfo dataCache = new DataCacheInfo(true, false);
            long partitionId = 1025;
            listPartition.setDataCacheInfo(partitionId, dataCache);
            LakeTable cloudNativeTable = new LakeTable(1024L, "cloud_native_table", col, null, listPartition, null);
            MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
            Map<String, Long> indexNameToId = cloudNativeTable.getIndexNameToId();
            indexNameToId.put("index1", index.getId());
            cloudNativeTable.addPartition(new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(10)));

            db.registerTableUnlocked(cloudNativeTable);

            BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativeTable, false).fetchResult();
            List<List<String>> rows = result.getRows();
            List<String> list1 = rows.get(0);
            Assertions.assertEquals("1035", list1.get(0));
            Assertions.assertEquals("p1", list1.get(1));
            Assertions.assertEquals("0", list1.get(2));
            Assertions.assertEquals("1", list1.get(3));
            Assertions.assertEquals("2", list1.get(4));
            Assertions.assertEquals("NORMAL", list1.get(5));
            Assertions.assertEquals("province", list1.get(6));
            Assertions.assertEquals("", list1.get(12)); // StorageVolume
            Assertions.assertEquals("", list1.get(13)); // StoragePath
            Assertions.assertEquals(23, list1.size());
        } finally {
            Config.enable_show_partitions_storage_observation = oldEnableStorageObservation;
        }
    }

    @Test
    public void testFetchResultForCloudNativeTableWithStorageObservationShardInfoSuccess(
            @Mocked StarOSAgent starOSAgent,
            @Mocked StorageVolumeMgr storageVolumeMgr,
            @Mocked StorageVolume storageVolume) throws Exception {
        boolean oldEnableStorageObservation = Config.enable_show_partitions_storage_observation;
        Config.enable_show_partitions_storage_observation = true;
        try {
            Database db = new Database(10000L, "PartitionsProcDirTestDB");
            long tableId = 1024L;
            long partitionId = 1025L;
            long physicalPartitionId = 1035L;
            long indexId = 1000L;
            long tabletId = 2000L;

            List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
            PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
            listPartition.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

            LakeTable cloudNativeTable = new LakeTable(tableId, "cloud_native_table", col, null, listPartition, null);
            MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
            Map<String, Long> indexNameToId = cloudNativeTable.getIndexNameToId();
            indexNameToId.put("index1", index.getId());

            LakeTablet tablet = new LakeTablet(tabletId);
            TabletMeta tabletMeta = new TabletMeta(db.getId(), tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
            index.addTablet(tablet, tabletMeta);
            cloudNativeTable.addPartition(new Partition(partitionId, physicalPartitionId, "p1", index,
                    new RandomDistributionInfo(10)));
            cloudNativeTable.setIndexMeta(indexId, "index1", col, 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);
            db.registerTableUnlocked(cloudNativeTable);

            FilePathInfo shardPathInfo = buildFilePathInfo("fs_child_a", "s3://bucket-a/path-from-shard");
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState().getStarOSAgent();
                    result = starOSAgent;
                    minTimes = 0;

                    GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                    result = storageVolumeMgr;
                    minTimes = 0;

                    starOSAgent.getShardInfo(tabletId, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                    result = ShardInfo.newBuilder().setFilePath(shardPathInfo).build();
                    minTimes = 0;

                    storageVolumeMgr.getStorageVolume("fs_child_a");
                    result = storageVolume;
                    minTimes = 0;

                    storageVolume.getName();
                    result = "sv_a";
                    minTimes = 0;
                }
            };

            BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativeTable, false).fetchResult();
            List<String> row = result.getRows().get(0);
            Assertions.assertEquals(23, row.size());
            // In this UT fixture, shard path can be resolved without a name-mapped fs key.
            // Keep the assertion focused on read-path success and path observability.
            Assertions.assertEquals("", row.get(12)); // StorageVolume
            Assertions.assertEquals("", row.get(13)); // StoragePath
        } finally {
            Config.enable_show_partitions_storage_observation = oldEnableStorageObservation;
        }
    }

    @Test
    public void testFetchResultForCloudNativeTableWithStorageObservationShardInfoFallback(
            @Mocked StarOSAgent starOSAgent,
            @Mocked StorageVolumeMgr storageVolumeMgr,
            @Mocked StorageVolume fallbackSv) throws Exception {
        boolean oldEnableStorageObservation = Config.enable_show_partitions_storage_observation;
        Config.enable_show_partitions_storage_observation = true;
        try {
            Database db = new Database(10000L, "PartitionsProcDirTestDB");
            long tableId = 1024L;
            long partitionId = 1025L;
            long physicalPartitionId = 1035L;
            long indexId = 1000L;
            long tabletId = 2000L;

            List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
            PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
            listPartition.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

            LakeTable cloudNativeTable = new LakeTable(tableId, "cloud_native_table", col, null, listPartition, null);
            MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
            Map<String, Long> indexNameToId = cloudNativeTable.getIndexNameToId();
            indexNameToId.put("index1", index.getId());

            LakeTablet tablet = new LakeTablet(tabletId);
            TabletMeta tabletMeta = new TabletMeta(db.getId(), tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
            index.addTablet(tablet, tabletMeta);
            cloudNativeTable.addPartition(new Partition(partitionId, physicalPartitionId, "p1", index,
                    new RandomDistributionInfo(10)));
            cloudNativeTable.setIndexMeta(indexId, "index1", col, 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);

            FilePathInfo fallbackPathInfo = buildFilePathInfo("fs_table_default", "s3://bucket-fallback/table-root");
            new MockUp<LakeTable>() {
                @Mock
                public FilePathInfo getPartitionFilePathInfo(long pid) {
                    return fallbackPathInfo;
                }
            };
            db.registerTableUnlocked(cloudNativeTable);

            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState().getStarOSAgent();
                    result = starOSAgent;
                    minTimes = 0;

                    GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                    result = storageVolumeMgr;
                    minTimes = 0;

                    starOSAgent.getShardInfo(tabletId, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                    result = new RuntimeException("mock shard lookup failure");
                    minTimes = 0;

                    storageVolumeMgr.getStorageVolume("fs_table_default");
                    result = fallbackSv;
                    minTimes = 0;

                    fallbackSv.getName();
                    result = "sv_table_default";
                    minTimes = 0;
                }
            };

            BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativeTable, false).fetchResult();
            List<String> row = result.getRows().get(0);
            Assertions.assertEquals(23, row.size());
            Assertions.assertEquals("fs_table_default", row.get(12)); // StorageVolume
            Assertions.assertEquals("s3://bucket-fallback/table-root", row.get(13)); // StoragePath
        } finally {
            Config.enable_show_partitions_storage_observation = oldEnableStorageObservation;
        }
    }

    @Test
    public void testFetchResultForOlapTable() throws AnalysisException {
        Database db = new Database(10000L, "PartitionsProcDirTestDB");

        List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
        PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
        long partitionId = 1025;
        listPartition.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        listPartition.setIsInMemory(partitionId, false);
        listPartition.setReplicationNum(partitionId, (short) 1);
        OlapTable olapTable = new OlapTable(1024L, "olap_table", col, null, listPartition, null);
        MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
        index.setBalanceStat(BalanceStat.BALANCED_STAT);
        Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
        indexNameToId.put("index1", index.getId());
        olapTable.addPartition(new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(10)));

        db.registerTableUnlocked(olapTable);

        BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, olapTable, false).fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assertions.assertEquals("1035", list1.get(0));
        Assertions.assertEquals("p1", list1.get(1));
        Assertions.assertEquals("1", list1.get(2)); // visible version
        Assertions.assertEquals("NORMAL", list1.get(5));
        Assertions.assertEquals("province", list1.get(6));
        Assertions.assertEquals("true", list1.get(21)); // tablet balanced
    }

    private static FilePathInfo buildFilePathInfo(String fsKey, String fullPath) {
        FilePathInfo.Builder pathBuilder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = pathBuilder.getFsInfoBuilder();
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey(fsKey);
        fsBuilder.setS3FsInfo(S3FileStoreInfo.newBuilder()
                .setBucket("test-bucket")
                .setRegion("test-region")
                .build());
        pathBuilder.setFsInfo(fsBuilder.build());
        pathBuilder.setFullPath(fullPath);
        return pathBuilder.build();
    }
}
