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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OdpsConnectorInternalMgr {
    private final String catalogName;
    private final Map<String, String> properties;
    
    private ExecutorService pullRemoteFileExecutor;
    
    private final int loadRemoteFileMetadataThreadNum;
    
    public OdpsConnectorInternalMgr(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.loadRemoteFileMetadataThreadNum = Integer.parseInt(properties.getOrDefault(
                OdpsProperties.REMOTE_FILE_LOAD_THREAD_NUM,
                String.valueOf(Config.remote_file_metadata_load_concurrency)));
    }
    
    public void shutdown() {
        if (pullRemoteFileExecutor != null) {
            pullRemoteFileExecutor.shutdown();
        }
    }
    
    public ExecutorService getPullRemoteFileExecutor() {
        if (pullRemoteFileExecutor == null) {
            pullRemoteFileExecutor = Executors.newFixedThreadPool(loadRemoteFileMetadataThreadNum,
                    new ThreadFactoryBuilder().setNameFormat("pull-odps-remote-files-%d").build());
        }
        
        return pullRemoteFileExecutor;
    }
}