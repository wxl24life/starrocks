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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AIModelResource;
import com.starrocks.catalog.Resource;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * AI Project plan node. Extends ProjectNode to inherit runtime filter
 * pushdown, slot equivalence propagation, query cache normalization,
 * and output slot enumeration. Only toThrift() is overridden to
 * generate TAIProjectNode (not TProjectNode) with AI model configurations.
 * <p>
 * All AI model configurations (SYSTEM and RESOURCE) are collected into a
 * unified map keyed by config ID, passed to BE via TAIProjectNode.ai_model_configs.
 * <p>
 * On the BE side, AIProjectNode::decompose_to_pipeline() splits it into
 * two pipelines:
 * <pre>
 *   Pipeline 1 (upstream):   [child operators] → AIBufferSinkOperator
 *   Pipeline 2 (downstream): AIScanOperator → [LimitOperator]
 * </pre>
 * The two pipelines are connected through a shared AIChunkBuffer. AI function
 * evaluation happens in the ScanExecutor IO thread pool, fully integrated with
 * WorkGroup-based fair scheduling (CFS vruntime).
 */
public class AIProjectNode extends ProjectNode {
    private static final Logger LOG = LogManager.getLogger(AIProjectNode.class);
    public static final String SYSTEM_CONFIG_ID = "__system__";

    public AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                         Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap) {
        super(id, tupleDescriptor, child, slotMap, commonSlotMap);
        this.planNodeName = "AI Project";
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.AI_PROJECT_NODE;
        msg.ai_project_node = new TAIProjectNode();
        getSlotMap().forEach((key, value) ->
                msg.ai_project_node.putToSlot_map(key.asInt(), value.treeToThrift()));
        getCommonSlotMap().forEach((key, value) ->
                msg.ai_project_node.putToCommon_slot_map(key.asInt(), value.treeToThrift()));

        Map<String, TAIModelConfiguration> configs = new HashMap<>();
        configs.put(SYSTEM_CONFIG_ID, buildSystemConfig());
        collectResourceConfigs(configs, getSlotMap());
        collectResourceConfigs(configs, getCommonSlotMap());
        msg.ai_project_node.setAi_model_configs(configs);
    }

    private TAIModelConfiguration buildSystemConfig() {
        TAIModelConfiguration config = new TAIModelConfiguration();
        String endpoint = Config.ai_default_model_endpoint;
        String model = Config.ai_default_model_name;
        String apiKey = Strings.nullToEmpty(System.getenv("AI_FUNCTION_MODEL_API_KEY"));
        if (endpoint.isEmpty()) {
            LOG.warn("AI system config: ai_default_model_endpoint is empty");
        }
        config.setEndpoint(endpoint);
        config.setModel(model);
        config.setApi_key(apiKey);
        config.setProvider("openai_compatible");
        config.setExtra_params("");
        return config;
    }

    private void collectResourceConfigs(Map<String, TAIModelConfiguration> configs,
                                        Map<SlotId, Expr> slotMap) {
        if (slotMap == null) {
            return;
        }
        for (Expr expr : slotMap.values()) {
            collectFromExprTree(configs, expr);
        }
    }

    private void collectFromExprTree(Map<String, TAIModelConfiguration> configs, Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fnCall = (FunctionCallExpr) expr;
            String configId = fnCall.getAiModelConfigId();
            if (configId != null && !SYSTEM_CONFIG_ID.equals(configId) && !configs.containsKey(configId)) {
                Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(configId);
                Preconditions.checkState(resource instanceof AIModelResource,
                        "AI model resource '%s' not found or not an AI_MODEL resource", configId);
                AIModelResource aiResource = (AIModelResource) resource;
                TAIModelConfiguration config = new TAIModelConfiguration();
                config.setEndpoint(aiResource.getEndpoint());
                config.setModel(aiResource.getModel());
                config.setApi_key(Strings.nullToEmpty(aiResource.getApiKey()));
                config.setExtra_params(Strings.nullToEmpty(aiResource.getExtraParams()));
                config.setProvider(aiResource.getProvider());
                configs.put(configId, config);
            }
        }
        for (Expr child : expr.getChildren()) {
            collectFromExprTree(configs, child);
        }
    }

    @Override
    public boolean isTrivial() {
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        // AI projections are non-deterministic (external HTTP calls) and must
        // not participate in query cache digest computation. Use the PlanNode
        // default (no-op) so fragments containing AI functions are never cached.
    }
}
