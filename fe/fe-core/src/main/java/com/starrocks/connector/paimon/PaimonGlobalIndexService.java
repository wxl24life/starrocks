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
import com.starrocks.common.util.DlfUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.proto.PPaimonGlobalIndexEvaluateRequest;
import com.starrocks.proto.PPaimonGlobalIndexEvaluateResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.InternalSqlExecutor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Range;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
     * R6 — direct FE→BE bypass for the inner SQL "SELECT to_base64(index_result)
     * FROM <table>$global_index" path. Resolves shard ranges via IndexAnalyzer, serialises the
     * paimon cloud config + condition JSON, fires one brpc per shard to a single alive BE, and
     * aggregates results through the same GlobalIndexResultAggregator the SQL path uses.
     *
     * <p>Returns null on any failure so the caller can transparently fall back to the SQL path.
     * Returns an empty bitmap when no shards exist (consistent with PaimonUtils empty behaviour).
     */
    private Object evaluateViaBypass() {
        try {
            FileStoreTable nativeTable = (FileStoreTable) paimonTable.getNativeTable();
            String tablePath = nativeTable.location().toString();

            ConnectorMetadata metadata = GlobalStateMgr.getCurrentState().getConnectorMgr()
                    .getConnector(paimonTable.getCatalogName()).getMetadata();
            List<Range> shards = new IndexAnalyzer(paimonTable, metadata).getIndexShards(it -> (Range) it);
            if (shards.isEmpty()) {
                return PaimonUtils.createEmptyGlobalIndexResult(indexCondition);
            }

            CloudConfiguration cc = DlfUtil.buildPaimonCloudConfiguration(paimonTable);
            TCloudConfiguration tCloudConf = new TCloudConfiguration();
            if (cc != null) {
                cc.toThrift(tCloudConf);
            }
            byte[] cloudConfBytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(tCloudConf);

            Map<String, Object> queryJson = indexCondition.toQueryJson();
            String args = JSONObject.valueToString(queryJson);

            SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            List<Long> beIds = sysInfo.getBackendIds(true);
            if (beIds.isEmpty()) {
                LOG.warn("evaluateViaBypass: no alive BE");
                return null;
            }
            // PoC scope: route every shard to the first alive BE. Production should distribute via
            // PaimonGlobalIndexBackendSelector's logic; the win we are measuring is RPC overhead,
            // not placement, so single-BE is enough to validate the contention thesis.
            Backend be = sysInfo.getBackend(beIds.get(0));
            if (be == null) {
                return null;
            }
            TNetworkAddress beAddr = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            PaimonUtils.GlobalIndexResultAggregator agg = null;
            for (int i = 0; i < shards.size(); i++) {
                Range range = shards.get(i);
                PPaimonGlobalIndexEvaluateRequest req = new PPaimonGlobalIndexEvaluateRequest();
                req.indexConditionJson = args;
                req.tablePath = tablePath;
                req.cloudConfigurationThrift = cloudConfBytes;
                req.rangeFrom = range.from;
                req.rangeTo = range.to;
                req.shardId = (long) i;

                PPaimonGlobalIndexEvaluateResponse resp = BackendServiceClient.getInstance()
                        .paimonGlobalIndexEvaluateAsync(beAddr, req).get(60, TimeUnit.SECONDS);

                StatusPB status = resp.status;
                if (status == null || (status.statusCode != null && status.statusCode != 0)) {
                    LOG.warn("paimon GI evaluate RPC non-OK: shard={} status={} msgs={}",
                            i, status == null ? "?" : status.statusCode,
                            status == null ? "?" : status.errorMsgs);
                    return null;
                }

                if (resp.indexResult != null && resp.indexResult.length > 0) {
                    GlobalIndexResult partial = new GlobalIndexResultSerializer()
                            .deserializeFromBytes(resp.indexResult);
                    if (agg == null) {
                        agg = PaimonUtils.createGlobalIndexResultAggregator(partial, indexCondition);
                    } else {
                        agg.iterate(partial);
                    }
                }
            }
            return agg == null ? null : agg.terminate();
        } catch (Exception e) {
            LOG.warn("evaluateViaBypass failed, will fall back to SQL", e);
            return null;
        }
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
