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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class UnifiedJobsProcDirTest {

    private static final long TEST_DB_ID = 10000L;

    private UnifiedJobsProcDir unifiedJobsProcDir;

    @BeforeEach
    public void setUp() {
        unifiedJobsProcDir = new UnifiedJobsProcDir(TEST_DB_ID, false);
    }

    @Test
    public void testTitleNames() {
        // Verify title names are correctly defined
        Assertions.assertEquals(11, UnifiedJobsProcDir.TITLE_NAMES.size());
        Assertions.assertEquals("JobId", UnifiedJobsProcDir.TITLE_NAMES.get(0));
        Assertions.assertEquals("JobType", UnifiedJobsProcDir.TITLE_NAMES.get(1));
        Assertions.assertEquals("Label", UnifiedJobsProcDir.TITLE_NAMES.get(2));
        Assertions.assertEquals("TableName", UnifiedJobsProcDir.TITLE_NAMES.get(3));
        Assertions.assertEquals("State", UnifiedJobsProcDir.TITLE_NAMES.get(4));
        Assertions.assertEquals("WatershedTxnId", UnifiedJobsProcDir.TITLE_NAMES.get(5));
        Assertions.assertEquals("Progress", UnifiedJobsProcDir.TITLE_NAMES.get(6));
        Assertions.assertEquals("CreateTime", UnifiedJobsProcDir.TITLE_NAMES.get(7));
        Assertions.assertEquals("FinishTime", UnifiedJobsProcDir.TITLE_NAMES.get(8));
        Assertions.assertEquals("Timeout(s)", UnifiedJobsProcDir.TITLE_NAMES.get(9));
        Assertions.assertEquals("ErrorMsg", UnifiedJobsProcDir.TITLE_NAMES.get(10));
    }

    @Test
    public void testJobTypeConstants() {
        Assertions.assertEquals("TRANSACTION", UnifiedJobsProcDir.JOB_TYPE_TRANSACTION);
        Assertions.assertEquals("SCHEMA_CHANGE", UnifiedJobsProcDir.JOB_TYPE_SCHEMA_CHANGE);
        Assertions.assertEquals("ROLLUP", UnifiedJobsProcDir.JOB_TYPE_ROLLUP);
        Assertions.assertEquals("OPTIMIZE", UnifiedJobsProcDir.JOB_TYPE_OPTIMIZE);
    }

    @Test
    public void testFetchResultWithTransactions() throws AnalysisException {
        // Create already converted transaction data
        List<List<String>> txnJobs = new ArrayList<>();
        txnJobs.add(createUnifiedJobRow("1001", "TRANSACTION", "load_label_1", "",
                "COMMITTED", "1001", "", "2025-01-08 10:00:00", "N/A", "3600", ""));

        new Expectations(unifiedJobsProcDir) {
            {
                unifiedJobsProcDir.collectTransactionStateJobs();
                result = txnJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectSchemaChangeJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectRollupJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectOptimizeJobs();
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        ProcResult result = unifiedJobsProcDir.fetchResult();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(UnifiedJobsProcDir.TITLE_NAMES, result.getColumnNames());
        Assertions.assertEquals(1, result.getRows().size());

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals("1001", row.get(0));                          // JobId
        Assertions.assertEquals("TRANSACTION", row.get(1));                   // JobType
    }

    @Test
    public void testFetchResultWithSchemaChangeJobs() throws AnalysisException {
        // Mock schema change job data
        List<List<String>> scJobs = new ArrayList<>();
        scJobs.add(createUnifiedJobRow("2001", "SCHEMA_CHANGE", "new_index", "test_table",
                "RUNNING", "12345", "50/100", "2025-01-08 09:00:00", "N/A", "86400", ""));

        new Expectations(unifiedJobsProcDir) {
            {
                unifiedJobsProcDir.collectTransactionStateJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectSchemaChangeJobs();
                result = scJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectRollupJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectOptimizeJobs();
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        ProcResult result = unifiedJobsProcDir.fetchResult();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getRows().size());

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals("2001", row.get(0));                          // JobId
        Assertions.assertEquals("SCHEMA_CHANGE", row.get(1));                 // JobType
        Assertions.assertEquals("new_index", row.get(2));                     // Label (IndexName)
        Assertions.assertEquals("test_table", row.get(3));                    // TableName
        Assertions.assertEquals("RUNNING", row.get(4));                       // State
    }

    @Test
    public void testFetchResultWithRollupJobs() throws AnalysisException {
        // Mock rollup job data
        List<List<String>> rollupJobs = new ArrayList<>();
        rollupJobs.add(createUnifiedJobRow("3001", "ROLLUP", "rollup_idx", "base_table",
                "PENDING", "23456", "", "2025-01-08 08:00:00", "N/A", "86400", ""));

        new Expectations(unifiedJobsProcDir) {
            {
                unifiedJobsProcDir.collectTransactionStateJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectSchemaChangeJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectRollupJobs();
                result = rollupJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectOptimizeJobs();
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        ProcResult result = unifiedJobsProcDir.fetchResult();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getRows().size());

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals("3001", row.get(0));                          // JobId
        Assertions.assertEquals("ROLLUP", row.get(1));                        // JobType
        Assertions.assertEquals("rollup_idx", row.get(2));                    // Label
        Assertions.assertEquals("base_table", row.get(3));                    // TableName
        Assertions.assertEquals("PENDING", row.get(4));                       // State
    }

    @Test
    public void testFetchResultWithOptimizeJobs() throws AnalysisException {
        // Mock optimize job data
        List<List<String>> optimizeJobs = new ArrayList<>();
        optimizeJobs.add(createUnifiedJobRow("4001", "OPTIMIZE", "REORDER_COLUMN", "orders",
                "WAITING_TXN", "34567", "", "2025-01-08 07:00:00", "N/A", "86400", ""));

        new Expectations(unifiedJobsProcDir) {
            {
                unifiedJobsProcDir.collectTransactionStateJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectSchemaChangeJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectRollupJobs();
                result = new ArrayList<>();
                minTimes = 0;

                unifiedJobsProcDir.collectOptimizeJobs();
                result = optimizeJobs;
                minTimes = 0;
            }
        };

        ProcResult result = unifiedJobsProcDir.fetchResult();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getRows().size());

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals("4001", row.get(0));                          // JobId
        Assertions.assertEquals("OPTIMIZE", row.get(1));                      // JobType
        Assertions.assertEquals("REORDER_COLUMN", row.get(2));                // Label (Operation)
        Assertions.assertEquals("orders", row.get(3));                        // TableName
        Assertions.assertEquals("WAITING_TXN", row.get(4));                   // State
    }

    @Test
    public void testFetchResultWithMixedJobs() throws AnalysisException {
        // Mock mixed job data
        List<List<String>> txnJobs = new ArrayList<>();
        txnJobs.add(createUnifiedJobRow("1001", "TRANSACTION", "load_1", "",
                "COMMITTED", "1001", "", "2025-01-08 10:00:00", "N/A", "3600", ""));

        List<List<String>> scJobs = new ArrayList<>();
        scJobs.add(createUnifiedJobRow("2001", "SCHEMA_CHANGE", "idx1", "table1",
                "RUNNING", "12345", "50/100", "2025-01-08 09:00:00", "N/A", "86400", ""));

        List<List<String>> rollupJobs = new ArrayList<>();
        rollupJobs.add(createUnifiedJobRow("3001", "ROLLUP", "rollup1", "table2",
                "PENDING", "23456", "", "2025-01-08 08:00:00", "N/A", "86400", ""));

        new Expectations(unifiedJobsProcDir) {
            {
                unifiedJobsProcDir.collectTransactionStateJobs();
                result = txnJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectSchemaChangeJobs();
                result = scJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectRollupJobs();
                result = rollupJobs;
                minTimes = 0;

                unifiedJobsProcDir.collectOptimizeJobs();
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        ProcResult result = unifiedJobsProcDir.fetchResult();

        Assertions.assertNotNull(result);
        // Should have 3 jobs: 1 transaction + 1 schema change + 1 rollup
        Assertions.assertEquals(3, result.getRows().size());

        // Verify jobs are sorted by CreateTime descending
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals("TRANSACTION", rows.get(0).get(1));   // 10:00:00 - newest
        Assertions.assertEquals("SCHEMA_CHANGE", rows.get(1).get(1)); // 09:00:00
        Assertions.assertEquals("ROLLUP", rows.get(2).get(1));        // 08:00:00 - oldest
    }

    @Test
    public void testLookupAll() throws AnalysisException {
        ProcNodeInterface node = unifiedJobsProcDir.lookup("all");

        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof UnifiedJobsProcDir);
    }

    @Test
    public void testLookupInvalidName() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            unifiedJobsProcDir.lookup("invalid");
        });
    }

    @Test
    public void testLookupEmptyName() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            unifiedJobsProcDir.lookup("");
        });

        Assertions.assertThrows(AnalysisException.class, () -> {
            unifiedJobsProcDir.lookup(null);
        });
    }

    @Test
    public void testRegister() {
        Assertions.assertFalse(unifiedJobsProcDir.register("test", null));
    }

    @Test
    public void testResultLimit() throws AnalysisException {
        int originalLimit = Config.max_show_proc_transactions_entry;

        try {
            Config.max_show_proc_transactions_entry = 2;

            // Create 5 jobs
            List<List<String>> manyJobs = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                manyJobs.add(createUnifiedJobRow(String.valueOf(1000 + i), "TRANSACTION", "label_" + i, "",
                        "COMMITTED", String.valueOf(1000 + i), "",
                        "2025-01-08 10:0" + i + ":00", "N/A", "3600", ""));
            }

            new Expectations(unifiedJobsProcDir) {
                {
                    unifiedJobsProcDir.collectTransactionStateJobs();
                    result = manyJobs;
                    minTimes = 0;

                    unifiedJobsProcDir.collectSchemaChangeJobs();
                    result = new ArrayList<>();
                    minTimes = 0;

                    unifiedJobsProcDir.collectRollupJobs();
                    result = new ArrayList<>();
                    minTimes = 0;

                    unifiedJobsProcDir.collectOptimizeJobs();
                    result = new ArrayList<>();
                    minTimes = 0;
                }
            };

            ProcResult result = unifiedJobsProcDir.fetchResult();

            // Should be limited to 2
            Assertions.assertEquals(2, result.getRows().size());
        } finally {
            Config.max_show_proc_transactions_entry = originalLimit;
        }
    }

    @Test
    public void testConvertTransactionToUnifiedFormat() {
        List<String> txnInfo = createMockTransactionInfo("1001", "test_label", "VISIBLE",
                "2025-01-08 10:00:00", "2025-01-08 10:05:00", "7200000", "test error");

        List<String> unified = unifiedJobsProcDir.convertTransactionToUnifiedFormat(txnInfo);

        Assertions.assertEquals(11, unified.size());
        Assertions.assertEquals("1001", unified.get(0));                      // JobId
        Assertions.assertEquals("TRANSACTION", unified.get(1));               // JobType
        Assertions.assertEquals("test_label", unified.get(2));                // Label
        Assertions.assertEquals("", unified.get(3));                          // TableName
        Assertions.assertEquals("VISIBLE", unified.get(4));                   // State
        Assertions.assertEquals("1001", unified.get(5));                      // WatershedTxnId
        Assertions.assertEquals("", unified.get(6));                          // Progress
        Assertions.assertEquals("2025-01-08 10:00:00", unified.get(7));       // CreateTime
        Assertions.assertEquals("2025-01-08 10:05:00", unified.get(8));       // FinishTime
        Assertions.assertEquals("7200", unified.get(9));                      // Timeout(s)
        Assertions.assertEquals("test error", unified.get(10));               // ErrorMsg
    }

    @Test
    public void testConvertSchemaChangeToUnifiedFormat() {
        List<Comparable> scInfo = createMockSchemaChangeInfo("2001", "test_table",
                "2025-01-08 09:00:00", "2025-01-08 10:00:00", "new_index",
                "12345", "FINISHED", "success", "100/100", "86400");

        List<String> unified = unifiedJobsProcDir.convertSchemaChangeToUnifiedFormat(scInfo);

        Assertions.assertEquals(11, unified.size());
        Assertions.assertEquals("2001", unified.get(0));                      // JobId
        Assertions.assertEquals("SCHEMA_CHANGE", unified.get(1));             // JobType
        Assertions.assertEquals("new_index", unified.get(2));                 // Label
        Assertions.assertEquals("test_table", unified.get(3));                // TableName
        Assertions.assertEquals("FINISHED", unified.get(4));                  // State
        Assertions.assertEquals("12345", unified.get(5));                     // WatershedTxnId
        Assertions.assertEquals("100/100", unified.get(6));                   // Progress
        Assertions.assertEquals("2025-01-08 09:00:00", unified.get(7));       // CreateTime
        Assertions.assertEquals("2025-01-08 10:00:00", unified.get(8));       // FinishTime
        Assertions.assertEquals("86400", unified.get(9));                     // Timeout
        Assertions.assertEquals("success", unified.get(10));                  // ErrorMsg
    }

    @Test
    public void testConvertRollupToUnifiedFormat() {
        List<Comparable> rollupInfo = createMockRollupInfo("3001", "base_table",
                "2025-01-08 08:00:00", "2025-01-08 09:00:00", "rollup_idx",
                "23456", "FINISHED", "", "100/100", "86400");

        List<String> unified = unifiedJobsProcDir.convertRollupToUnifiedFormat(rollupInfo);

        Assertions.assertEquals(11, unified.size());
        Assertions.assertEquals("3001", unified.get(0));                      // JobId
        Assertions.assertEquals("ROLLUP", unified.get(1));                    // JobType
        Assertions.assertEquals("rollup_idx", unified.get(2));                // Label
        Assertions.assertEquals("base_table", unified.get(3));                // TableName
        Assertions.assertEquals("FINISHED", unified.get(4));                  // State
        Assertions.assertEquals("23456", unified.get(5));                     // WatershedTxnId
        Assertions.assertEquals("100/100", unified.get(6));                   // Progress
    }

    @Test
    public void testConvertOptimizeToUnifiedFormat() {
        List<Comparable> optInfo = createMockOptimizeInfo("4001", "orders",
                "2025-01-08 07:00:00", "2025-01-08 08:00:00", "REORDER_COLUMN",
                "34567", "FINISHED", "", "100/100", "86400");

        List<String> unified = unifiedJobsProcDir.convertOptimizeToUnifiedFormat(optInfo);

        Assertions.assertEquals(11, unified.size());
        Assertions.assertEquals("4001", unified.get(0));                      // JobId
        Assertions.assertEquals("OPTIMIZE", unified.get(1));                  // JobType
        Assertions.assertEquals("REORDER_COLUMN", unified.get(2));            // Label
        Assertions.assertEquals("orders", unified.get(3));                    // TableName
        Assertions.assertEquals("FINISHED", unified.get(4));                  // State
        Assertions.assertEquals("34567", unified.get(5));                     // WatershedTxnId
    }

    // Helper methods

    private List<String> createMockTransactionInfo(String txnId, String label, String status,
                                                   String prepareTime, String finishTime,
                                                   String timeoutMs, String errMsg) {
        List<String> info = new ArrayList<>();
        info.add(txnId);           // 0: TransactionId
        info.add(label);           // 1: Label
        info.add("FE");            // 2: Coordinator
        info.add(status);          // 3: TransactionStatus
        info.add("FRONTEND");      // 4: LoadJobSourceType
        info.add(prepareTime);     // 5: PrepareTime
        info.add(null);            // 6: PreparedTime
        info.add(null);            // 7: CommitTime
        info.add(null);            // 8: PublishTime
        info.add(finishTime);      // 9: FinishTime
        info.add(null);            // 10: Reason
        info.add("0");             // 11: ErrorReplicasCount
        info.add("1");             // 12: ListenerId
        info.add(timeoutMs);       // 13: TimeoutMs
        info.add(null);            // 14: PreparedTimeoutMs
        info.add(errMsg);          // 15: ErrMsg
        return info;
    }

    private List<Comparable> createMockSchemaChangeInfo(String jobId, String tableName,
                                                        String createTime, String finishTime,
                                                        String indexName, String txnId,
                                                        String state, String msg,
                                                        String progress, String timeout) {
        List<Comparable> info = new ArrayList<>();
        info.add(Long.parseLong(jobId));  // 0: JobId
        info.add(tableName);              // 1: TableName
        info.add(createTime);             // 2: CreateTime
        info.add(finishTime != null ? finishTime : "N/A"); // 3: FinishTime
        info.add(indexName);              // 4: IndexName
        info.add(100L);                   // 5: IndexId
        info.add(99L);                    // 6: OriginIndexId
        info.add("1:0");                  // 7: SchemaVersion
        info.add(Long.parseLong(txnId));  // 8: TransactionId
        info.add(state);                  // 9: State
        info.add(msg);                    // 10: Msg
        info.add(progress);               // 11: Progress
        info.add(Long.parseLong(timeout)); // 12: Timeout
        return info;
    }

    private List<Comparable> createMockRollupInfo(String jobId, String tableName,
                                                  String createTime, String finishTime,
                                                  String rollupIndexName, String txnId,
                                                  String state, String msg,
                                                  String progress, String timeout) {
        List<Comparable> info = new ArrayList<>();
        info.add(Long.parseLong(jobId));  // 0: JobId
        info.add(tableName);              // 1: TableName
        info.add(createTime);             // 2: CreateTime
        info.add(finishTime != null ? finishTime : "N/A"); // 3: FinishedTime
        info.add("base_index");           // 4: BaseIndexName
        info.add(rollupIndexName);        // 5: RollupIndexName
        info.add(100L);                   // 6: RollupId
        info.add(Long.parseLong(txnId));  // 7: TransactionId
        info.add(state);                  // 8: State
        info.add(msg);                    // 9: Msg
        info.add(progress);               // 10: Progress
        info.add(Long.parseLong(timeout)); // 11: Timeout
        return info;
    }

    private List<Comparable> createMockOptimizeInfo(String jobId, String tableName,
                                                    String createTime, String finishTime,
                                                    String operation, String txnId,
                                                    String state, String msg,
                                                    String progress, String timeout) {
        List<Comparable> info = new ArrayList<>();
        info.add(Long.parseLong(jobId));  // 0: JobId
        info.add(tableName);              // 1: TableName
        info.add(createTime);             // 2: CreateTime
        info.add(finishTime != null ? finishTime : "N/A"); // 3: FinishTime
        info.add(operation);              // 4: Operation
        info.add(Long.parseLong(txnId));  // 5: TransactionId
        info.add(state);                  // 6: State
        info.add(msg);                    // 7: Msg
        info.add(progress);               // 8: Progress
        info.add(Long.parseLong(timeout)); // 9: Timeout
        return info;
    }

    private List<String> createUnifiedJobRow(String jobId, String jobType, String label,
                                             String tableName, String state, String watershedTxnId,
                                             String progress, String createTime, String finishTime,
                                             String timeout, String errorMsg) {
        List<String> row = Lists.newArrayList();
        row.add(jobId);
        row.add(jobType);
        row.add(label);
        row.add(tableName);
        row.add(state);
        row.add(watershedTxnId);
        row.add(progress);
        row.add(createTime);
        row.add(finishTime);
        row.add(timeout);
        row.add(errorMsg);
        return row;
    }

}
