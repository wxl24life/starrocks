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

#include "exec/ai_project_node.h"

#include <algorithm>

#include "common/config.h"
#include "exec/pipeline/ai_buffer_sink_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/ai_chunk_buffer.h"
#include "exec/pipeline/scan/ai_morsel_queue.h"
#include "exec/pipeline/scan/ai_scan_context.h"
#include "exec/pipeline/scan/ai_scan_operator.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exprs/ai_function_call_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace starrocks {

AIProjectNode::AIProjectNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc)
        : ExecNode(pool, node, desc), _desc_tbl(desc) {}

AIProjectNode::~AIProjectNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status AIProjectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    const auto& ai_node = tnode.ai_project_node;

    size_t column_size = ai_node.slot_map.size();
    _expr_ctxs.reserve(column_size);
    _slot_ids.reserve(column_size);
    _type_is_nullable.reserve(column_size);

    std::map<SlotId, bool> slot_null_mapping;
    for (auto const& slot : row_desc().tuple_descriptors()[0]->slots()) {
        slot_null_mapping[slot->id()] = slot->is_nullable();
    }

    for (auto const& [key, val] : ai_node.slot_map) {
        _slot_ids.emplace_back(key);
        ExprContext* context;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, val, &context, state, true));
        _expr_ctxs.emplace_back(context);
        _type_is_nullable.emplace_back(slot_null_mapping[key]);
    }

    size_t common_sub_column_size = ai_node.common_slot_map.size();
    _common_sub_expr_ctxs.reserve(common_sub_column_size);
    _common_sub_slot_ids.reserve(common_sub_column_size);

    for (auto const& [key, val] : ai_node.common_slot_map) {
        ExprContext* context;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, val, &context, state, true));
        _common_sub_slot_ids.emplace_back(key);
        _common_sub_expr_ctxs.emplace_back(context);
    }

    if (ai_node.__isset.ai_model_configs) {
        for (const auto& [config_id, tc] : ai_node.ai_model_configs) {
            auto config = std::make_shared<AIModelConfiguration>();
            if (tc.__isset.endpoint) config->endpoint = tc.endpoint;
            if (tc.__isset.api_key) config->api_key = tc.api_key;
            if (tc.__isset.model) config->model = tc.model;
            if (tc.__isset.extra_params) config->extra_params = tc.extra_params;
            if (tc.__isset.provider) config->provider = tc.provider;
            _ai_model_configs[config_id] = std::move(config);
        }
    }

    _ai_scan_node = std::make_unique<AIScanNode>(_pool, tnode, _desc_tbl);

    return Status::OK();
}

Status AIProjectNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_common_sub_expr_ctxs, state));

    return Status::OK();
}

Status AIProjectNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_children[0]->open(state));

    RETURN_IF_ERROR(Expr::open(_common_sub_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));
    return Status::OK();
}

Status AIProjectNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("AIProjectNode::get_next is not supported; use pipeline execution");
}

void AIProjectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    Expr::close(_expr_ctxs, state);
    Expr::close(_common_sub_expr_ctxs, state);

    ExecNode::close(state);
}

// Splits the AI project into two pipelines connected by an AIChunkBuffer:
//
//   Pipeline 1 (upstream):  [...child operators...] → AIBufferSinkOperator
//                           Produces raw chunks and pushes them into AIChunkBuffer.
//
//   Pipeline 2 (downstream): AIScanOperator → [LimitOperator]
//                            Consumes chunks from AIChunkBuffer, evaluates AI
//                            expressions in the ScanExecutor's IO thread pool
//                            (fair-scheduled via WorkGroup), and outputs results.
pipeline::OpFactories AIProjectNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    // Align with OlapScanNode/ConnectorScanNode: set the execution group
    // so that pipelines are added to the correct group (important for
    // colocate execution and resource isolation).
    auto exec_group = context->find_exec_group_by_plan_node_id(_id);
    context->set_current_execution_group(exec_group);

    // === Pipeline 1 (upstream): [child operators] → AIBufferSinkOperator ===
    OpFactories upstream_ops = _children[0]->decompose_to_pipeline(context);

    auto* upstream_source = context->source_operator(upstream_ops);
    size_t upstream_dop = upstream_source->degree_of_parallelism();

    std::string ai_diag_tag;
    ai_diag_tag.reserve(128);
    ai_diag_tag += "query_id=";
    ai_diag_tag += print_id(context->runtime_state()->query_id());
    ai_diag_tag += " fragment_instance_id=";
    ai_diag_tag += print_id(context->fragment_context()->fragment_instance_id());
    ai_diag_tag += " plan_node_id=";
    ai_diag_tag += std::to_string(_id);
    ai_diag_tag += ' ';

    VLOG(1) << "[AI] phase=decompose_ai_project " << ai_diag_tag
             << "context_dop=" << context->degree_of_parallelism()
             << " upstream_source_dop=" << upstream_dop << " limit=" << limit()
             << " num_exprs=" << _expr_ctxs.size()
             << " num_common_sub_exprs=" << _common_sub_expr_ctxs.size();

    size_t sub_chunk_size = AIBufferSinkOperator::sub_chunk_size();
    size_t chunk_size = context->runtime_state()->chunk_size();
    size_t sub_chunks_per_chunk = (chunk_size + sub_chunk_size - 1) / sub_chunk_size;

    // Compute downstream DOP early so we can size the buffer based on it.
    size_t max_ai_dop_for_buf = std::max<size_t>(1, config::ai_function_scan_thread_num);
    size_t downstream_dop_for_buf = std::min(upstream_dop, max_ai_dop_for_buf);

    // AI HTTP processing is the bottleneck (seconds per sub-chunk), so the
    // buffer only needs a few sub-chunks per downstream driver to keep them
    // busy.  A large buffer wastes memory — the upstream scan fills it in
    // milliseconds but the downstream takes minutes to drain it.
    // With yield-aware scheduling and 64-row sub-chunks, the downstream
    // processes sub-chunks faster (non-blocking dispatch), so keep a slightly
    // deeper buffer to avoid starving the downstream pipeline.
    static constexpr size_t kSubChunksPerDriver = 6;
    static constexpr size_t kMinBufferCapacity = 12;
    size_t ai_buffer_capacity = downstream_dop_for_buf * kSubChunksPerDriver;
    ai_buffer_capacity = std::max<size_t>(ai_buffer_capacity, kMinBufferCapacity);

    // Memory-based backpressure for AIChunkBuffer: clamp(query_mem * 2%, 32MB, 256MB).
    // AI HTTP processing is the bottleneck (~2s per sub-chunk at 64 QPS with 128 rows),
    // so the buffer holds at most a few sub-chunks per downstream driver. The mem_limit
    // is a safety net for abnormally wide rows; under normal conditions the capacity
    // constraint (5-40 MB actual) binds first.
    static constexpr double kAIBufferMemRatio = 0.02;                    // 2% of query memory
    auto* query_ctx_ptr = context->runtime_state()->query_ctx();
    int64_t query_mem = query_ctx_ptr ? query_ctx_ptr->get_static_query_mem_limit() : 0;
    static constexpr int64_t kMinAIBufferMemLimit = 32L * 1024 * 1024;   // 32 MB
    static constexpr int64_t kMaxAIBufferMemLimit = 256L * 1024 * 1024;  // 256 MB
    int64_t ai_buffer_mem_limit = kMinAIBufferMemLimit;
    if (query_mem > 0) {
        ai_buffer_mem_limit = std::clamp(static_cast<int64_t>(query_mem * kAIBufferMemRatio),
                                         kMinAIBufferMemLimit, kMaxAIBufferMemLimit);
    }

    auto ai_chunk_buffer = std::make_shared<AIChunkBuffer>(
            ai_buffer_capacity, static_cast<int>(upstream_dop), ai_buffer_mem_limit);

    upstream_ops.emplace_back(std::make_shared<AIBufferSinkOperatorFactory>(
            context->next_operator_id(), id(), ai_chunk_buffer, ai_diag_tag));

    context->add_pipeline(upstream_ops);

    // === Pre-compute is_ai_expr flags ===
    std::vector<bool> is_ai_expr(_expr_ctxs.size(), false);
    for (size_t i = 0; i < _expr_ctxs.size(); ++i) {
        if (dynamic_cast<AIFunctionCallExpr*>(_expr_ctxs[i]->root()) != nullptr) {
            is_ai_expr[i] = true;
        }
    }

    // === Pipeline 2 (downstream): AIScanOperator → [Limit] → [CollectStats] ===
    // DOP already computed above for buffer sizing.
    size_t dop = downstream_dop_for_buf;

    // Align with OlapScanNode/ConnectorScanNode: apply scan_use_query_mem_ratio
    // and kChunkBufferMemRatio to the scan memory limit, so the chunk buffer
    // does not consume a disproportionate share of query memory.
    static constexpr double kChunkBufferMemRatio = 0.5;
    int64_t scan_mem_limit = 0;
    auto* query_ctx = context->runtime_state()->query_ctx();
    if (query_ctx != nullptr) {
        scan_mem_limit = query_ctx->get_static_query_mem_limit() * config::scan_use_query_mem_ratio;
    }

    // Align with OlapScanNode/ConnectorScanNode: distinguish max vs default
    // buffer capacity so DynamicChunkBufferLimiter can ramp up gradually
    // instead of starting at full capacity.
    size_t max_buffer_capacity = ScanOperator::max_buffer_capacity() * dop;
    size_t default_buffer_capacity = std::min<size_t>(max_buffer_capacity, dop * sub_chunks_per_chunk);
    ChunkBufferLimiterPtr buffer_limiter = std::make_unique<DynamicChunkBufferLimiter>(
            max_buffer_capacity, default_buffer_capacity, static_cast<int64_t>(scan_mem_limit * kChunkBufferMemRatio),
            chunk_size);

    VLOG(1) << "[AI] phase=ai_pipeline_config " << ai_diag_tag << "upstream_dop=" << upstream_dop
             << " downstream_dop=" << dop << " ai_buffer_capacity=" << ai_buffer_capacity
             << " ai_buffer_mem_limit=" << ai_buffer_mem_limit
             << " max_buffer_cap=" << max_buffer_capacity << " default_buffer_cap=" << default_buffer_capacity
             << " scan_mem_limit=" << scan_mem_limit << " sub_chunk_size=" << sub_chunk_size
             << " chunk_size=" << chunk_size << " exec_group=" << exec_group;

    auto scan_op_factory = std::make_shared<AIScanOperatorFactory>(
            context->next_operator_id(), _ai_scan_node.get(), dop, ai_chunk_buffer, std::move(_slot_ids),
            std::move(_expr_ctxs), std::move(_type_is_nullable), std::move(_common_sub_slot_ids),
            std::move(_common_sub_expr_ctxs), std::move(is_ai_expr), std::move(_ai_model_configs),
            std::move(buffer_limiter), limit());

    // Ownership transferred to AIScanOperatorFactory; clear to prevent
    // AIProjectNode::close() from calling Expr::close() on stale pointers.
    _expr_ctxs.clear();
    _common_sub_expr_ctxs.clear();

    // Register AIMorselQueueFactory in fragment context's map so it stays alive
    // and gets wired to the pipeline during instantiation.
    auto morsel_queue_factory = std::make_unique<AIMorselQueueFactory>(id(), ai_chunk_buffer, dop);
    auto& morsel_queue_factories = context->fragment_context()->morsel_queue_factories();
    morsel_queue_factories.emplace(id(), std::move(morsel_queue_factory));

    scan_op_factory->set_degree_of_parallelism(dop);

    // Build the downstream pipeline operators
    OpFactories downstream_ops;
    downstream_ops.emplace_back(std::move(scan_op_factory));

    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    this->init_runtime_filter_for_operator(downstream_ops.back().get(), context, rc_rf_probe_collector);

    if (limit() != -1) {
        downstream_ops.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    // Align with decompose_scan_node_to_pipeline: insert CollectStats operator
    // for adaptive DOP support and query profile scan statistics.
    downstream_ops = context->maybe_interpolate_collect_stats(context->runtime_state(), id(), downstream_ops);

    return downstream_ops;
}

} // namespace starrocks
