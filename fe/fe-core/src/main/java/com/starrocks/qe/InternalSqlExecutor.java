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

package com.starrocks.qe;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Run an internal SQL and stream the result rows to an {@link Aggregator}.
 *
 * <p>Implementation is structurally equivalent to {@link SimpleExecutor#executeDQL}
 * with {@link TResultSinkType#HTTP_PROTOCAL}, but kept separate so we own the
 * inner ConnectContext / query id and can correlate it with the outer caller's
 * query id in logs (oncall needs both ids to trace a single ANN query end-to-end).
 *
 * <p>Each invocation logs one INFO line with {@code outer_query_id} (the user's
 * original ANN query) and {@code inner_query_id} (the internal SQL we issue
 * against {@code <table>$global_index}).
 *
 * <p><b>Binary columns:</b> ndjson is text-only (cast_expr_json.cpp puts
 * BinaryColumn slices directly into JSON values, which is unsafe for non-UTF-8
 * bytes). Caller wraps VARBINARY in {@code to_base64(col)} and decodes inside
 * {@link Aggregator#visitString}.
 */
public class InternalSqlExecutor {

    private static final Logger LOG = LogManager.getLogger(InternalSqlExecutor.class);

    public interface Aggregator<T> {
        default void visitString(int columnId, String value) {
            throw new RuntimeException("not implemented");
        }

        default void visitNull(int columnId) {
            throw new RuntimeException("not implemented");
        }

        default T terminate() {
            throw new RuntimeException("not implemented");
        }
    }

    public <T> T execute(String sql, Aggregator<T> aggregator) {
        ConnectContext outer = ConnectContext.get();
        String outerQueryId = (outer != null && outer.getQueryId() != null)
                ? DebugUtil.printId(outer.getQueryId()) : "<none>";
        UUID innerUuid = UUIDUtil.genUUID();
        String innerQueryId = DebugUtil.printId(innerUuid);
        String redactedSql = SqlCredentialRedactor.redact(sql);

        LOG.info("InternalSqlExecutor: outer_query_id={} inner_query_id={} sql={}",
                outerQueryId, innerQueryId, redactedSql);

        List<TResultBatch> batches = runDQL(sql, innerUuid, outer, outerQueryId, innerQueryId, redactedSql);

        if (batches != null) {
            for (TResultBatch batch : batches) {
                if (batch == null || batch.getRows() == null) {
                    continue;
                }
                for (ByteBuffer buffer : batch.getRows()) {
                    dispatchRow(buffer, aggregator);
                }
            }
        }
        return aggregator.terminate();
    }

    private static List<TResultBatch> runDQL(String sql, UUID innerUuid, ConnectContext outer,
                                             String outerQueryId, String innerQueryId, String redactedSql) {
        try {
            ConnectContext context = StatisticUtils.buildConnectContext(TResultSinkType.HTTP_PROTOCAL);
            if (outer != null) {
                if (outer.getSessionVariable().isEnableProfile()) {
                    context.getSessionVariable().setEnableProfile(true);
                }
                // Inherit the outer user/role so the inner SQL is checked against the same RBAC
                // context as the user-issued query, instead of running as the statistics user.
                if (outer.getCurrentUserIdentity() != null) {
                    context.setQualifiedUser(outer.getQualifiedUser());
                    context.setCurrentUserIdentity(outer.getCurrentUserIdentity());
                    context.setCurrentRoleIds(outer.getCurrentRoleIds());
                }
            }
            context.setThreadLocalInfo();
            context.setNeedQueued(false);
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(innerUuid);
            AuditLog.getInternalAudit().info(
                    "InternalSqlExecutor execute SQL | outer_query_id {} | inner_query_id {} | DQL {}",
                    outerQueryId, innerQueryId, redactedSql);
            Pair<List<TResultBatch>, Status> result = executor.executeStmtWithExecPlan(context, execPlan);
            if (result == null || result.second == null || !result.second.ok()) {
                String reason = (result == null || result.second == null) ? "no status" : result.second.getErrorMsg();
                throw new RuntimeException(String.format(
                        "InternalSqlExecutor failed: outer_query_id=%s inner_query_id=%s status=%s sql=%s",
                        outerQueryId, innerQueryId, reason, redactedSql));
            }
            collectProfile(context, executor, execPlan, parsedStmt);
            return result.first;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "InternalSqlExecutor failed: outer_query_id=%s inner_query_id=%s sql=%s",
                    outerQueryId, innerQueryId, redactedSql), e);
        } finally {
            ConnectContext.remove();
            if (outer != null) {
                outer.setThreadLocalInfo();
            }
        }
    }

    private static void collectProfile(ConnectContext context, StmtExecutor executor,
                                        ExecPlan execPlan, StatementBase parsedStmt) {
        if (!context.getSessionVariable().isEnableProfile() || executor.getCoordinator() == null) {
            return;
        }
        try {
            RuntimeProfile profile = new RuntimeProfile("Query");
            RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
            summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(context.getExecutionId()));
            summaryProfile.addInfoString(ProfileManager.START_TIME,
                    TimeUtils.longToTimeString(context.getStartTime()));
            long now = System.currentTimeMillis();
            summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(now));
            summaryProfile.addInfoString(ProfileManager.TOTAL_TIME,
                    DebugUtil.getPrettyStringMs(now - context.getStartTime()));
            summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
            summaryProfile.addInfoString(ProfileManager.QUERY_STATE, context.getState().toProfileString());
            summaryProfile.addInfoString(ProfileManager.USER, context.getQualifiedUser());
            summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, context.getDatabase());
            summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT,
                    AstToSQLBuilder.toSQLOrDefault(parsedStmt, ""));
            profile.addChild(summaryProfile);
            profile.addChild(executor.getCoordinator().buildQueryProfile(context.needMergeProfile()));
            ProfileManager.getInstance().pushProfile(
                    execPlan != null ? execPlan.getProfilingPlan() : null, profile);
        } catch (Exception e) {
            LOG.warn("Failed to collect profile for inner query {}", DebugUtil.printId(context.getExecutionId()), e);
        }
    }

    private static <T> void dispatchRow(ByteBuffer buffer, Aggregator<T> aggregator) {
        ByteBuffer dup = buffer.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        String row = new String(bytes, StandardCharsets.UTF_8).trim();
        if (row.isEmpty()) {
            return;
        }
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(row);
        } catch (Exception e) {
            throw new RuntimeException("failed to parse ndjson row: " + row, e);
        }
        if (!parsed.isJsonObject()) {
            return;
        }
        JsonObject obj = parsed.getAsJsonObject();
        JsonElement dataElement = obj.get("data");
        if (dataElement == null || !dataElement.isJsonArray()) {
            return;
        }
        JsonArray data = dataElement.getAsJsonArray();
        for (int i = 0; i < data.size(); i++) {
            JsonElement element = data.get(i);
            if (element == null || element.isJsonNull()) {
                aggregator.visitNull(i);
                continue;
            }
            if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
                aggregator.visitString(i, element.getAsString());
            } else {
                // Numbers, booleans, and nested objects/arrays — pass raw json text.
                aggregator.visitString(i, element.toString());
            }
        }
    }
}
