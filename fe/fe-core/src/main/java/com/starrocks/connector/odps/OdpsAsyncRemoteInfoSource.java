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

package com.starrocks.connector.odps;

import com.starrocks.catalog.Table;
import com.starrocks.connector.AsyncTaskQueue;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Executor;

public class OdpsAsyncRemoteInfoSource extends AsyncTaskQueue<RemoteFileInfo> implements RemoteFileInfoSource {
    private static final Logger LOG = LogManager.getLogger(OdpsAsyncRemoteInfoSource.class);

    private final Table table;
    private final GetRemoteFilesParams params;
    private final OdpsMetadata odpsMetadata;

    public OdpsAsyncRemoteInfoSource(Executor executor, Table table, GetRemoteFilesParams params, OdpsMetadata odpsMetadata) {
        super(executor);
        this.table = table;
        this.params = params;
        this.odpsMetadata = odpsMetadata;
    }

    @Override
    public RemoteFileInfo getOutput() {
        List<RemoteFileInfo> res = getOutputs(1);
        return res.get(0);
    }

    @Override
    public int computeOutputSize(RemoteFileInfo output) {
        return 1;
    }

    class GetRemoteFilesTask implements AsyncTaskQueue.Task<RemoteFileInfo> {
        @Override
        public List<RemoteFileInfo> run() {
            try {
                List<RemoteFileInfo> fileInfos = odpsMetadata.getRemoteFiles(table, params);
                return fileInfos;
            } catch (Exception e) {
                LOG.error("Failed to get remote files for table: {}", table.getName(), e);
                throw e;
            }
        }
    }

    public void run() {
        GetRemoteFilesTask task = new GetRemoteFilesTask();
        start(List.of(task));
    }
}