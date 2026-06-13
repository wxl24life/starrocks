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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.InternalSqlExecutor;
import com.starrocks.qe.SessionVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static com.starrocks.common.profile.Tracers.Module.INDEX;

/**
 * Phase 1 of the Paimon Global Index two-stage flow.
 *
 * <p>Issues an internal SQL against the virtual {@code <table>$global_index} table to evaluate
 * the index condition (predicate / topN ANN) on each shard. Each BE returns a serialized
 * {@code GlobalIndexResult} bitmap. Multiple shard results are aggregated into a single
 * {@code GlobalIndexResult} which is then passed to {@code getRemoteFiles} via
 * {@code GetRemoteFilesParams.globalIndexResult} to drive {@code DataEvolutionBatchScan}.
 *
 * <p>Extracted from {@code PaimonScanNode} so the scan node only orchestrates the call and the
 * service owns SQL construction, internal execution and shard-result aggregation.
 */
public class PaimonGlobalIndexService {

    private static final Logger LOG = LogManager.getLogger(PaimonGlobalIndexService.class);

    private final PaimonTable paimonTable;
    private final IndexCondition indexCondition;

    public PaimonGlobalIndexService(PaimonTable paimonTable, IndexCondition indexCondition) {
        this.paimonTable = paimonTable;
        this.indexCondition = indexCondition;
    }

    /**
     * Run the per-shard internal SQL, aggregate the shard results, and return a GlobalIndexResult
     * suitable for {@code GetRemoteFilesParams.globalIndexResult}. Never returns null: when every
     * shard returns NULL (no matches), an empty bitmap is returned so DataEvolutionBatchScan
     * produces zero-row splits instead of failing the query.
     */
    public Object evaluate() {
        String tracePrefix = "Paimon.GlobalIndex." + paimonTable.getCatalogTableName() + ".";

        // R6 PoC — opt-in bypass: skip inner SQL plan/dispatch and call BE evaluator via brpc.
        // Default off. Even when on, fall back to SQL path if bypass returns null (PoC stub state).
        ConnectContext ctx = ConnectContext.get();
        SessionVariable sv = ctx != null ? ctx.getSessionVariable() : null;
        if (sv != null && sv.isPaimonGlobalIndexUseThriftBypass()) {
            try (Timer ignored = Tracers.watchScope(INDEX, tracePrefix + "evaluate.bypass")) {
                Object bypassed = evaluateViaBypass();
                if (bypassed != null) {
                    return bypassed;
                }
                LOG.warn("paimon_global_index_use_thrift_bypass=true but bypass returned null; "
                        + "falling back to SQL path for table {}", paimonTable.getCatalogTableName());
            }
        }

        String sql;
        try (Timer ignored = Tracers.watchScope(INDEX, tracePrefix + "evaluate.buildSql")) {
            sql = buildSql();
        }
        String debugCondition = indexCondition.toDebugString();
        LOG.debug("evaluateGlobalIndexResult sql: {} condition: {}", sql, debugCondition);
        Tracers.record(INDEX, tracePrefix + "evaluateGlobalIndex.condition", debugCondition);

        Object aggregated;
        try (Timer ignored = Tracers.watchScope(INDEX, tracePrefix + "evaluate.internalSqlExec")) {
            aggregated = new InternalSqlExecutor().execute(sql, new ShardResultAggregator(indexCondition));
        }
        if (aggregated != null) {
            return aggregated;
        }
        try (Timer ignored = Tracers.watchScope(INDEX, tracePrefix + "evaluate.emptyResult")) {
            return PaimonUtils.createEmptyGlobalIndexResult(indexCondition);
        }
    }

    /**
     * R6 PoC stub. When the BE evaluator is wired (PInternalService.paimon_global_index_evaluate),
     * this builds PPaimonGlobalIndexEvaluateRequest, picks an alive BE, awaits the RPC, and
     * deserialises the response into a GlobalIndexResult.
     *
     * <p>Returns null to indicate "bypass not available" — caller falls back to the SQL path so
     * functional correctness is preserved during PoC roll-out.
     */
    private Object evaluateViaBypass() {
        // TODO(R6): implement once BE handler is no longer Unimplemented.
        // 1. resolve target BE via systemInfoService.getBackendIds(true)
        // 2. build PPaimonGlobalIndexEvaluateRequest from paimonTable + indexCondition + sessionVar
        // 3. BackendServiceClient.getInstance().paimonGlobalIndexEvaluateAsync(beAddr, req).get(timeout)
        // 4. deserialize index_result via GlobalIndexResultSerializer
        return null;
    }

    // index_result is VARBINARY; HTTP_PROTOCAL ndjson is text-only, so we wrap it in to_base64()
    // and decode in the visitor (see InternalSqlExecutor's "Binary columns" note).
    private String buildSql() {
        Map<String, Object> queryJson = indexCondition.toQueryJson();
        // Escape single quotes so any string literal inside the JSON cannot break out of the
        // SQL string (and hence cannot inject SQL via column names / string predicate constants).
        String args = JSONObject.valueToString(queryJson).replace("'", "''");
        return String.format("select to_base64(%s) from `%s`.`%s`.`%s%s` where args='%s';",
                IndexTable.INDEX_RESULT_COLUMN_NAME,
                paimonTable.getCatalogName(),
                paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(),
                IndexTable.INDEX_TABLE_SUFFIX,
                args);
    }

    private static class ShardResultAggregator implements InternalSqlExecutor.Aggregator<GlobalIndexResult> {

        private final IndexCondition indexCondition;
        private PaimonUtils.GlobalIndexResultAggregator agg;

        ShardResultAggregator(IndexCondition indexCondition) {
            this.indexCondition = indexCondition;
        }

        @Override
        public void visitString(int columnId, String value) {
            byte[] bytes = Base64.getDecoder().decode(value);
            try {
                GlobalIndexResultSerializer ser = new GlobalIndexResultSerializer();
                GlobalIndexResult partial = ser.deserializeFromBytes(bytes);
                if (agg == null) {
                    agg = PaimonUtils.createGlobalIndexResultAggregator(partial, indexCondition);
                } else {
                    agg.iterate(partial);
                }
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize GlobalIndexResult: " + e.getMessage(), e);
            }
        }

        @Override
        public void visitNull(int columnId) {
            // index_result NULL means this shard had no matches; skip silently.
        }

        @Override
        public GlobalIndexResult terminate() {
            return agg == null ? null : agg.terminate();
        }
    }
}
