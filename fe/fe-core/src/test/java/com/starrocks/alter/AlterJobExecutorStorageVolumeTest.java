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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.SetTableStorageVolumeLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class AlterJobExecutorStorageVolumeTest {
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final String OLD_SV_ID = "sv-old-id";
    @Mocked
    private MetadataMgr metadataMgr;
    @Mocked
    private StorageVolumeMgr storageVolumeMgr;
    @Mocked
    private LocalMetastore localMetastore;
    @Mocked
    private EditLog editLog;

    private Database db;
    private OlapTable table;
    private AlterJobExecutor executor;

    @BeforeEach
    public void setUp() {
        db = new Database(DB_ID, "db_ut");
        table = new OlapTable();
        table.setId(TABLE_ID);
        table.setName("tbl_ut");

        executor = new AlterJobExecutor();
        executor.db = db;
        executor.table = table;

        new MockUp<GlobalStateMgr>() {
            @Mock
            public MetadataMgr getMetadataMgr() {
                return metadataMgr;
            }

            @Mock
            public StorageVolumeMgr getStorageVolumeMgr() {
                return storageVolumeMgr;
            }

            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }

            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                metadataMgr.getDb(DB_ID);
                result = db;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testAlterToNonExistentSvThrows() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_missing"));

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_missing");
                result = null;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("not found"));

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 0;
                storageVolumeMgr.bindTableToStorageVolume((String) any, anyLong, anyLong);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterToDisabledSvThrows() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_disabled"));
        StorageVolume disabledSv = createS3StorageVolume("sv-disabled-id", "sv_disabled", false);

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_disabled");
                result = disabledSv;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("disabled"));

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 0;
                storageVolumeMgr.bindTableToStorageVolume((String) any, anyLong, anyLong);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterBindFailureRollsBack() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_new"));
        StorageVolume newSv = createS3StorageVolume("sv-new-id", "sv_new", true);
        StorageVolume oldSv = createS3StorageVolume(OLD_SV_ID, "sv_old", true);

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_new");
                result = newSv;

                storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID);
                result = OLD_SV_ID;

                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                result = false;

                storageVolumeMgr.getStorageVolume(OLD_SV_ID);
                result = oldSv;

                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                result = true;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("Failed to bind table to storage volume"));

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 1;
                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                times = 1;
                localMetastore.setLakeStorageInfo((Database) any, (OlapTable) any, (String) any, (Map<String, String>) any);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterBindFailureRollbackAlsoFails() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_new"));
        StorageVolume newSv = createS3StorageVolume("sv-new-id", "sv_new", true);
        StorageVolume oldSv = createS3StorageVolume(OLD_SV_ID, "sv_old", true);

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_new");
                result = newSv;

                storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID);
                result = OLD_SV_ID;
                result = null;

                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                result = false;

                storageVolumeMgr.getStorageVolume(OLD_SV_ID);
                result = oldSv;

                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                result = new DdlException("rollback bind failed");
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("Failed to bind table to storage volume"));

        Assertions.assertNull(storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID),
                "table should be diagnosable as unbound after rollback also fails");

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 1;
                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                times = 1;
                localMetastore.setLakeStorageInfo((Database) any, (OlapTable) any, (String) any, (Map<String, String>) any);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterBindFailureWithNullOldSvId() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_new"));
        StorageVolume newSv = createS3StorageVolume("sv-new-id", "sv_new", true);

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_new");
                result = newSv;

                // oldSvId == null branch: rollback should be skipped
                storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID);
                result = null;
                result = null;

                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                result = false;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("Failed to bind table to storage volume"));

        Assertions.assertNull(storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID),
                "table should remain unbound when oldSvId is null and bind to new SV fails");

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 1;
                storageVolumeMgr.getStorageVolume((String) any);
                times = 0;
                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                times = 0;
                localMetastore.setLakeStorageInfo((Database) any, (OlapTable) any, (String) any, (Map<String, String>) any);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterBindFailureWithMissingOldSvObject() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_new"));
        StorageVolume newSv = createS3StorageVolume("sv-new-id", "sv_new", true);

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_new");
                result = newSv;

                // oldSvId exists but oldSv object is missing during rollback
                storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID);
                result = OLD_SV_ID;

                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                result = false;

                storageVolumeMgr.getStorageVolume(OLD_SV_ID);
                result = null;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("Failed to bind table to storage volume"));

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 1;
                storageVolumeMgr.getStorageVolume(OLD_SV_ID);
                times = 1;
                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                times = 0;
                localMetastore.setLakeStorageInfo((Database) any, (OlapTable) any, (String) any, (Map<String, String>) any);
                times = 0;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterSetLakeStorageInfoFailureRollsBack() throws Exception {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(storageVolumeProps("sv_new"));
        StorageVolume newSv = createS3StorageVolume("sv-new-id", "sv_new", true);
        StorageVolume oldSv = createS3StorageVolume(OLD_SV_ID, "sv_old", true);
        String newSvId = "sv-new-id";

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName("sv_new");
                result = newSv;

                storageVolumeMgr.getStorageVolumeIdOfTable(TABLE_ID);
                result = OLD_SV_ID;
                result = newSvId;

                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                result = true;

                localMetastore.setLakeStorageInfo(db, table, newSvId, (Map<String, String>) any);
                result = new DdlException("set lake storage info failed");

                storageVolumeMgr.getStorageVolume(OLD_SV_ID);
                result = oldSv;

                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                result = true;
            }
        };

        AlterJobException ex = Assertions.assertThrows(AlterJobException.class,
                () -> executor.visitModifyTablePropertiesClause(clause, null));
        Assertions.assertTrue(ex.getMessage().contains("set lake storage info failed"));

        new Verifications() {
            {
                storageVolumeMgr.unbindTableToStorageVolume(TABLE_ID);
                times = 1;
                storageVolumeMgr.bindTableToStorageVolume("sv_new", DB_ID, TABLE_ID);
                times = 1;
                localMetastore.setLakeStorageInfo(db, table, newSvId, (Map<String, String>) any);
                times = 1;
                storageVolumeMgr.bindTableToStorageVolume("sv_old", DB_ID, TABLE_ID);
                times = 1;
                editLog.logSetTableStorageVolume((SetTableStorageVolumeLog) any);
                times = 0;
            }
        };
    }

    private static Map<String, String> storageVolumeProps(String volumeName) {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, volumeName);
        return properties;
    }

    private static StorageVolume createS3StorageVolume(String id, String name, boolean enabled) throws DdlException {
        Map<String, String> params = new HashMap<>();
        params.put(AWS_S3_REGION, "region");
        params.put(AWS_S3_ENDPOINT, "endpoint");
        params.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        return new StorageVolume(id, name, "S3", java.util.Arrays.asList("s3://bucket-ut"),
                params, enabled, "");
    }
}
