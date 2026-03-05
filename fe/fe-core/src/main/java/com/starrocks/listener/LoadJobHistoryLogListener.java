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


package com.starrocks.listener;

import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.qe.DmlType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Listener to trigger load_history log print after load finished
 */
public class LoadJobHistoryLogListener implements LoadJobListener {
    public static final LoadJobHistoryLogListener INSTANCE = new LoadJobHistoryLogListener();

    private static final Logger LOG = LogManager.getLogger(LoadJobHistoryLogListener.class);
    private static final Logger LOADS_HISTORY_LOG = LogManager.getLogger("loads_history");
    private static final Gson GSON = new Gson();

    private boolean needTrigger() {
        if (!Config.enable_loads_history_log) {
            return false;
        }
        if (GlobalStateMgr.isCheckpointThread()) {
            return false;
        }
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        if (stateMgr == null || !stateMgr.isLeader() || !stateMgr.isReady()) {
            return false;
        }
        return true;
    }

    @Override
    public void onStreamLoadTransactionFinish(TransactionState transactionState) {
        if (!needTrigger()) {
            return;
        }
        String label = transactionState.getLabel();
        List<StreamLoadTask> streamLoadTaskList = GlobalStateMgr.getCurrentState().getStreamLoadMgr()
                .getTaskByName(label);
        if (streamLoadTaskList != null) {
            streamLoadTaskList.parallelStream().forEach(streamLoadTask -> {
                String jsonString = GSON.toJson(streamLoadTask.toThrift());
                LOADS_HISTORY_LOG.info(jsonString);
            });
        }
    }

    @Override
    public void onLoadJobTransactionFinish(TransactionState transactionState) {
        // Handle routine load separately since it's managed by RoutineLoadMgr, not LoadMgr
        if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
            // For ROUTINE_LOAD_TASK, the callbackIdList always contains exactly one element (the RoutineLoadJob id),
            // which is set during RoutineLoadTaskInfo.beginTxn() via GlobalTransactionMgr.beginTransaction().
            doRoutineLoadJobLog(transactionState.getCallbackId().get(0));
        } else {
            doJobLog(transactionState.getLabel());
        }
    }


    @Override
    public void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table,
                                              DmlType dmlType) {
        // only need to log insert load job record after its stat has been updated
        // skip
    }

    @Override
    public void onDMLStmtFinishedUpdateJobStat(TransactionState transactionState, DmlType dmlType) {
        doJobLog(transactionState.getLabel());
    }

    @Override
    public void onInsertOverwriteJobCommitFinish(Database db, Table table, InsertOverwriteJobStats stats) {
        // skip
    }

    private void doRoutineLoadJobLog(long routineLoadJobId) {
        if (!needTrigger()) {
            return;
        }
        RoutineLoadJob routineLoadJob = GlobalStateMgr.getCurrentState().getRoutineLoadMgr().getJob(routineLoadJobId);
        if (routineLoadJob != null) {
            try {
                String jsonString = GSON.toJson(routineLoadJob.toThrift());
                LOADS_HISTORY_LOG.info(jsonString);
            } catch (Exception e) {
                LOG.warn("failed to log routine load job history, jobId: {}", routineLoadJobId, e);
            }
        }
    }

    private void doJobLog(String label) {
        if (!needTrigger()) {
            return;
        }
        List<LoadJob> loadJobs = GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobs(label);
        if (loadJobs != null) {
            loadJobs.parallelStream().forEach(loadJob -> {
                String jsonString = GSON.toJson(loadJob.toThrift());
                LOADS_HISTORY_LOG.info(jsonString);
            });
        }
    }

    @Override
    public void onDeleteJobTransactionFinish(Database db, Table table) {
        // do nothing
    }
}
