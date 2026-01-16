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

import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TransStateProcDirTest {

    private static final long TEST_DB_ID = 10000L;
    private static final String TEST_DB_NAME = "test_db";

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private GlobalTransactionMgr transactionMgr;

    @Mocked
    private LocalMetastore localMetastore;

    @Mocked
    private Database database;

    @BeforeEach
    public void setUp() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };
    }

    @Test
    public void testLookupRunning() throws AnalysisException {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;

                localMetastore.getDb(TEST_DB_NAME);
                result = database;
                minTimes = 0;

                database.getId();
                result = TEST_DB_ID;
                minTimes = 0;
            }
        };

        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);
        ProcNodeInterface node = procDir.lookup("running");

        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof TransProcDir);
    }

    @Test
    public void testLookupFinished() throws AnalysisException {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;

                localMetastore.getDb(TEST_DB_NAME);
                result = database;
                minTimes = 0;

                database.getId();
                result = TEST_DB_ID;
                minTimes = 0;
            }
        };

        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);
        ProcNodeInterface node = procDir.lookup("finished");

        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof TransProcDir);
    }

    @Test
    public void testLookupAllJobs() throws AnalysisException {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;

                localMetastore.getDb(TEST_DB_NAME);
                result = database;
                minTimes = 0;

                database.getId();
                result = TEST_DB_ID;
                minTimes = 0;
            }
        };

        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);
        ProcNodeInterface node = procDir.lookup("all_jobs");

        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof UnifiedJobsProcDir);
    }

    @Test
    public void testLookupInvalidState() {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;

                localMetastore.getDb(TEST_DB_NAME);
                result = database;
                minTimes = 0;

                database.getId();
                result = TEST_DB_ID;
                minTimes = 0;
            }
        };

        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            procDir.lookup("invalid_state");
        });

        Assertions.assertTrue(exception.getMessage().contains("State is invalid"));
        Assertions.assertTrue(exception.getMessage().contains("running, finished, all_jobs"));
    }

    @Test
    public void testLookupEmptyState() {
        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);

        Assertions.assertThrows(AnalysisException.class, () -> {
            procDir.lookup("");
        });

        Assertions.assertThrows(AnalysisException.class, () -> {
            procDir.lookup(null);
        });
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        List<List<String>> stateInfo = new ArrayList<>();
        List<String> runningInfo = new ArrayList<>();
        runningInfo.add("running");
        runningInfo.add("5");
        stateInfo.add(runningInfo);

        List<String> finishedInfo = new ArrayList<>();
        finishedInfo.add("finished");
        finishedInfo.add("10");
        stateInfo.add(finishedInfo);

        new Expectations() {
            {
                globalStateMgr.getGlobalTransactionMgr();
                result = transactionMgr;
                minTimes = 0;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;

                localMetastore.getDb(TEST_DB_NAME);
                result = database;
                minTimes = 0;

                database.getId();
                result = TEST_DB_ID;
                minTimes = 0;

                transactionMgr.getDbTransStateInfo(TEST_DB_ID);
                result = stateInfo;
                minTimes = 0;
            }
        };

        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);
        ProcResult result = procDir.fetchResult();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(TransStateProcDir.TITLE_NAMES, result.getColumnNames());
        Assertions.assertEquals(2, result.getRows().size());
    }

    @Test
    public void testRegister() {
        TransStateProcDir procDir = new TransStateProcDir(TEST_DB_NAME);
        Assertions.assertFalse(procDir.register("test", null));
    }
}

