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

package com.starrocks.lakeoptimizer;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.lakeoptimizer.cache.TableCacheKey;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager for LakeOptimizer async refresh tasks
 */
public class LakeOptimizerRefreshManager {
    private static final Logger LOG = LogManager.getLogger(LakeOptimizerRefreshManager.class);
    private static final String LOG_PREFIX = "[LakeOptimizer]";

    // Tracks tables that have pending or running refresh tasks.
    private final Set<TableCacheKey> pendingTables = ConcurrentHashMap.newKeySet();
    private final ExecutorService refreshExecutor;

    public LakeOptimizerRefreshManager() {
        this.refreshExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "lake-optimizer-refresh-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                });
    }

    public void triggerAsyncRefresh(PaimonTable table) {
        TableCacheKey key = new TableCacheKey(
                table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());

        if (!pendingTables.add(key)) {
            LOG.debug("{} Refresh already pending for {}, skip", LOG_PREFIX, key);
            return;
        }

        LOG.info("{} Triggering async refresh for {}, (current snapshot={})", LOG_PREFIX, key, table.getEndSnapshot());

        ConnectContext context = LakeOptimizerUtils.buildConnectContext();
        refreshExecutor.submit(() -> {
            try {
                context.setThreadLocalInfo();
                TableName tableName = new TableName(key.catalogName, key.dbName, key.tableName);
                GlobalStateMgr.getCurrentState().refreshLakeOptimizerTable(context, tableName, new ArrayList<>());
                LOG.info("{} Refresh completed for {}", LOG_PREFIX, key);
            } catch (Exception e) {
                LOG.error("{} Failed to execute refresh for {}", LOG_PREFIX, key, e);
            } finally {
                pendingTables.remove(key);
                ConnectContext.remove();
            }
        });
    }
}
