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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * UnifiedJobsProcDir provides a unified view of all job types including:
 * - TransactionState transactions (load, delete, compaction, etc.)
 * - AlterJobV2 jobs (schema change, rollup, optimize)
 *
 * Usage:
 *   SHOW PROC '/transactions/<dbId>/all_jobs'         -- show running jobs
 *   SHOW PROC '/transactions/<dbId>/all_jobs/all'     -- show all jobs including finished
 */
public class UnifiedJobsProcDir implements ProcDirInterface {

    // Job type constants
    public static final String JOB_TYPE_TRANSACTION = "TRANSACTION";
    public static final String JOB_TYPE_SCHEMA_CHANGE = "SCHEMA_CHANGE";
    public static final String JOB_TYPE_ROLLUP = "ROLLUP";
    public static final String JOB_TYPE_OPTIMIZE = "OPTIMIZE";

    // Unified column names for all job types
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId")
            .add("JobType")
            .add("Label")
            .add("TableName")
            .add("State")
            .add("WatershedTxnId")
            .add("Progress")
            .add("CreateTime")
            .add("FinishTime")
            .add("Timeout(s)")
            .add("ErrorMsg")
            .build();

    // Column indices for TransProcDir data
    // TransProcDir.TITLE_NAMES: TransactionId(0), Label(1), Coordinator(2), TransactionStatus(3),
    // LoadJobSourceType(4), PrepareTime(5), PreparedTime(6), CommitTime(7), PublishTime(8), 
    // FinishTime(9), Reason(10), ErrorReplicasCount(11), ListenerId(12), TimeoutMs(13), 
    // PreparedTimeoutMs(14), ErrMsg(15)
    private static final int TXN_IDX_TRANSACTION_ID = 0;
    private static final int TXN_IDX_LABEL = 1;
    private static final int TXN_IDX_STATUS = 3;
    private static final int TXN_IDX_PREPARE_TIME = 5;
    private static final int TXN_IDX_FINISH_TIME = 9;
    private static final int TXN_IDX_TIMEOUT_MS = 13;
    private static final int TXN_IDX_ERR_MSG = 15;

    // Column indices for SchemaChangeProcDir data
    // SchemaChangeProcDir.TITLE_NAMES: JobId(0), TableName(1), CreateTime(2), FinishTime(3),
    // IndexName(4), IndexId(5), OriginIndexId(6), SchemaVersion(7), TransactionId(8), 
    // State(9), Msg(10), Progress(11), Timeout(12), [Warehouse(13)]
    private static final int SC_IDX_JOB_ID = 0;
    private static final int SC_IDX_TABLE_NAME = 1;
    private static final int SC_IDX_CREATE_TIME = 2;
    private static final int SC_IDX_FINISH_TIME = 3;
    private static final int SC_IDX_INDEX_NAME = 4;
    private static final int SC_IDX_TXN_ID = 8;
    private static final int SC_IDX_STATE = 9;
    private static final int SC_IDX_MSG = 10;
    private static final int SC_IDX_PROGRESS = 11;
    private static final int SC_IDX_TIMEOUT = 12;

    // Column indices for RollupProcDir data
    // RollupProcDir.TITLE_NAMES: JobId(0), TableName(1), CreateTime(2), FinishedTime(3),
    // BaseIndexName(4), RollupIndexName(5), RollupId(6), TransactionId(7), 
    // State(8), Msg(9), Progress(10), Timeout(11)
    private static final int ROLLUP_IDX_JOB_ID = 0;
    private static final int ROLLUP_IDX_TABLE_NAME = 1;
    private static final int ROLLUP_IDX_CREATE_TIME = 2;
    private static final int ROLLUP_IDX_FINISH_TIME = 3;
    private static final int ROLLUP_IDX_ROLLUP_INDEX_NAME = 5;
    private static final int ROLLUP_IDX_TXN_ID = 7;
    private static final int ROLLUP_IDX_STATE = 8;
    private static final int ROLLUP_IDX_MSG = 9;
    private static final int ROLLUP_IDX_PROGRESS = 10;
    private static final int ROLLUP_IDX_TIMEOUT = 11;

    // Column indices for OptimizeProcDir data
    // OptimizeProcDir.TITLE_NAMES: JobId(0), TableName(1), CreateTime(2), FinishTime(3),
    // Operation(4), TransactionId(5), State(6), Msg(7), Progress(8), Timeout(9)
    private static final int OPT_IDX_JOB_ID = 0;
    private static final int OPT_IDX_TABLE_NAME = 1;
    private static final int OPT_IDX_CREATE_TIME = 2;
    private static final int OPT_IDX_FINISH_TIME = 3;
    private static final int OPT_IDX_OPERATION = 4;
    private static final int OPT_IDX_TXN_ID = 5;
    private static final int OPT_IDX_STATE = 6;
    private static final int OPT_IDX_MSG = 7;
    private static final int OPT_IDX_PROGRESS = 8;
    private static final int OPT_IDX_TIMEOUT = 9;

    private final long dbId;
    private final boolean includeFinished;

    public UnifiedJobsProcDir(long dbId, boolean includeFinished) {
        this.dbId = dbId;
        this.includeFinished = includeFinished;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<String>> allJobs = new ArrayList<>();

        // 1. Collect TransactionState transactions
        allJobs.addAll(collectTransactionStateJobs());

        // 2. Collect Schema Change jobs
        allJobs.addAll(collectSchemaChangeJobs());

        // 3. Collect Rollup jobs
        allJobs.addAll(collectRollupJobs());

        // 4. Collect Optimize jobs
        allJobs.addAll(collectOptimizeJobs());

        // Sort by CreateTime in descending order (newest first)
        allJobs.sort(Comparator.comparing((List<String> row) -> row.get(7)).reversed());

        // Limit the result count
        int limit = Config.max_show_proc_transactions_entry;
        if (allJobs.size() > limit) {
            allJobs = allJobs.subList(0, limit);
        }

        for (List<String> job : allJobs) {
            result.addRow(job);
        }
        return result;
    }

    /**
     * Collect TransactionState transactions from GlobalTransactionMgr
     */
    List<List<String>> collectTransactionStateJobs() {
        List<List<String>> jobs = new ArrayList<>();
        GlobalTransactionMgr txnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        try {
            // Get running transactions
            List<List<String>> runningInfos = txnMgr.getDbTransInfo(dbId, true,
                    Config.max_show_proc_transactions_entry);
            for (List<String> info : runningInfos) {
                jobs.add(convertTransactionToUnifiedFormat(info));
            }

            // If includeFinished, also get finished transactions
            if (includeFinished) {
                List<List<String>> finishedInfos = txnMgr.getDbTransInfo(dbId, false,
                        Config.max_show_proc_transactions_entry);
                for (List<String> info : finishedInfos) {
                    jobs.add(convertTransactionToUnifiedFormat(info));
                }
            }
        } catch (AnalysisException e) {
            // If db doesn't exist, return empty list
        }
        return jobs;
    }

    /**
     * Convert TransactionState info to unified format
     */
    List<String> convertTransactionToUnifiedFormat(List<String> txnInfo) {
        List<String> row = new ArrayList<>();
        row.add(safeGet(txnInfo, TXN_IDX_TRANSACTION_ID));      // JobId = TransactionId
        row.add(JOB_TYPE_TRANSACTION);                           // JobType
        row.add(safeGet(txnInfo, TXN_IDX_LABEL));               // Label
        row.add("");                                             // TableName (N/A for transaction)
        row.add(safeGet(txnInfo, TXN_IDX_STATUS));              // State = TransactionStatus
        row.add(safeGet(txnInfo, TXN_IDX_TRANSACTION_ID));      // WatershedTxnId = TransactionId itself
        row.add("");                                             // Progress
        row.add(safeGet(txnInfo, TXN_IDX_PREPARE_TIME));        // CreateTime = PrepareTime
        row.add(safeGet(txnInfo, TXN_IDX_FINISH_TIME));         // FinishTime
        row.add(convertMsToSeconds(safeGet(txnInfo, TXN_IDX_TIMEOUT_MS))); // Timeout(s)
        row.add(safeGet(txnInfo, TXN_IDX_ERR_MSG));             // ErrorMsg
        return row;
    }

    /**
     * Collect Schema Change jobs from SchemaChangeHandler
     */
    List<List<String>> collectSchemaChangeJobs() {
        List<List<String>> jobs = new ArrayList<>();
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return jobs;
        }

        List<List<Comparable>> jobInfos = handler.getAlterJobInfosByDb(db);
        for (List<Comparable> info : jobInfos) {
            String stateStr = safeGetComparable(info, SC_IDX_STATE);
            if (!includeFinished && isAlterJobFinished(stateStr)) {
                continue;
            }
            jobs.add(convertSchemaChangeToUnifiedFormat(info));
        }
        return jobs;
    }

    /**
     * Convert SchemaChange job info to unified format
     */
    List<String> convertSchemaChangeToUnifiedFormat(List<Comparable> info) {
        List<String> row = new ArrayList<>();
        row.add(safeGetComparable(info, SC_IDX_JOB_ID));        // JobId
        row.add(JOB_TYPE_SCHEMA_CHANGE);                         // JobType
        row.add(safeGetComparable(info, SC_IDX_INDEX_NAME));    // Label = IndexName
        row.add(safeGetComparable(info, SC_IDX_TABLE_NAME));    // TableName
        row.add(safeGetComparable(info, SC_IDX_STATE));         // State
        row.add(safeGetComparable(info, SC_IDX_TXN_ID));        // WatershedTxnId
        row.add(safeGetComparable(info, SC_IDX_PROGRESS));      // Progress
        row.add(safeGetComparable(info, SC_IDX_CREATE_TIME));   // CreateTime
        row.add(safeGetComparable(info, SC_IDX_FINISH_TIME));   // FinishTime
        row.add(safeGetComparable(info, SC_IDX_TIMEOUT));       // Timeout(s)
        row.add(safeGetComparable(info, SC_IDX_MSG));           // ErrorMsg
        return row;
    }

    /**
     * Collect Rollup jobs from MaterializedViewHandler
     */
    List<List<String>> collectRollupJobs() {
        List<List<String>> jobs = new ArrayList<>();
        MaterializedViewHandler handler = GlobalStateMgr.getCurrentState().getRollupHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return jobs;
        }

        List<List<Comparable>> jobInfos = handler.getAlterJobInfosByDb(db);
        for (List<Comparable> info : jobInfos) {
            String stateStr = safeGetComparable(info, ROLLUP_IDX_STATE);
            if (!includeFinished && isAlterJobFinished(stateStr)) {
                continue;
            }
            jobs.add(convertRollupToUnifiedFormat(info));
        }
        return jobs;
    }

    /**
     * Convert Rollup job info to unified format
     */
    List<String> convertRollupToUnifiedFormat(List<Comparable> info) {
        List<String> row = new ArrayList<>();
        row.add(safeGetComparable(info, ROLLUP_IDX_JOB_ID));            // JobId
        row.add(JOB_TYPE_ROLLUP);                                        // JobType
        row.add(safeGetComparable(info, ROLLUP_IDX_ROLLUP_INDEX_NAME)); // Label = RollupIndexName
        row.add(safeGetComparable(info, ROLLUP_IDX_TABLE_NAME));        // TableName
        row.add(safeGetComparable(info, ROLLUP_IDX_STATE));             // State
        row.add(safeGetComparable(info, ROLLUP_IDX_TXN_ID));            // WatershedTxnId
        row.add(safeGetComparable(info, ROLLUP_IDX_PROGRESS));          // Progress
        row.add(safeGetComparable(info, ROLLUP_IDX_CREATE_TIME));       // CreateTime
        row.add(safeGetComparable(info, ROLLUP_IDX_FINISH_TIME));       // FinishTime
        row.add(safeGetComparable(info, ROLLUP_IDX_TIMEOUT));           // Timeout(s)
        row.add(safeGetComparable(info, ROLLUP_IDX_MSG));               // ErrorMsg
        return row;
    }

    /**
     * Collect Optimize jobs from SchemaChangeHandler
     */
    List<List<String>> collectOptimizeJobs() {
        List<List<String>> jobs = new ArrayList<>();
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return jobs;
        }

        List<List<Comparable>> jobInfos = handler.getOptimizeJobInfosByDb(db);
        for (List<Comparable> info : jobInfos) {
            String stateStr = safeGetComparable(info, OPT_IDX_STATE);
            if (!includeFinished && isAlterJobFinished(stateStr)) {
                continue;
            }
            jobs.add(convertOptimizeToUnifiedFormat(info));
        }
        return jobs;
    }

    /**
     * Convert Optimize job info to unified format
     */
    List<String> convertOptimizeToUnifiedFormat(List<Comparable> info) {
        List<String> row = new ArrayList<>();
        row.add(safeGetComparable(info, OPT_IDX_JOB_ID));       // JobId
        row.add(JOB_TYPE_OPTIMIZE);                              // JobType
        row.add(safeGetComparable(info, OPT_IDX_OPERATION));    // Label = Operation
        row.add(safeGetComparable(info, OPT_IDX_TABLE_NAME));   // TableName
        row.add(safeGetComparable(info, OPT_IDX_STATE));        // State
        row.add(safeGetComparable(info, OPT_IDX_TXN_ID));       // WatershedTxnId
        row.add(safeGetComparable(info, OPT_IDX_PROGRESS));     // Progress
        row.add(safeGetComparable(info, OPT_IDX_CREATE_TIME));  // CreateTime
        row.add(safeGetComparable(info, OPT_IDX_FINISH_TIME));  // FinishTime
        row.add(safeGetComparable(info, OPT_IDX_TIMEOUT));      // Timeout(s)
        row.add(safeGetComparable(info, OPT_IDX_MSG));          // ErrorMsg
        return row;
    }

    /**
     * Check if an AlterJobV2 is in final state (FINISHED or CANCELLED)
     */
    private boolean isAlterJobFinished(String state) {
        if (Strings.isNullOrEmpty(state)) {
            return false;
        }
        try {
            AlterJobV2.JobState jobState = AlterJobV2.JobState.valueOf(state);
            return jobState.isFinalState();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Safely get element from list, return empty string if index out of bounds
     */
    private String safeGet(List<String> list, int index) {
        if (list == null || index < 0 || index >= list.size()) {
            return "";
        }
        String value = list.get(index);
        return value == null ? "" : value;
    }

    /**
     * Safely get element from Comparable list, convert to String
     */
    private String safeGetComparable(List<Comparable> list, int index) {
        if (list == null || index < 0 || index >= list.size()) {
            return "";
        }
        Comparable value = list.get(index);
        return value == null ? "" : value.toString();
    }

    /**
     * Convert milliseconds string to seconds string
     */
    private String convertMsToSeconds(String msStr) {
        if (Strings.isNullOrEmpty(msStr)) {
            return "";
        }
        try {
            long ms = Long.parseLong(msStr);
            return String.valueOf(ms / 1000);
        } catch (NumberFormatException e) {
            return msStr;
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("Name is not set");
        }

        if ("all".equalsIgnoreCase(name)) {
            // Return a new instance with includeFinished = true
            return new UnifiedJobsProcDir(dbId, true);
        }

        throw new AnalysisException("Invalid name: " + name + ". Only 'all' is supported.");
    }
}

