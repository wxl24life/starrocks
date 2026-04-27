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

#include "exec/pipeline/scan/ai_scan_operator.h"

#include <limits>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/scan/ai_chunk_source.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/ai_function_call_expr.h"
#include "exprs/expr.h"
#include "exprs/function_context.h"
#include "runtime/current_thread.h"
#include "runtime/global_dict/parser.h"
#include "util/bthreads/util.h"
#include "util/debug/query_trace.h"
#include "util/failpoint/fail_point.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

// ==================== AIScanOperatorFactory ====================

AIScanOperatorFactory::AIScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop,
                                             AIChunkBufferPtr ai_chunk_buffer, std::vector<int32_t> column_ids,
                                             std::vector<ExprContext*> expr_ctxs, std::vector<bool> type_is_nullable,
                                             std::vector<int32_t> common_sub_column_ids,
                                             std::vector<ExprContext*> common_sub_expr_ctxs,
                                             std::vector<bool> is_ai_expr,
                                             AIModelConfigMap ai_model_configs,
                                             ChunkBufferLimiterPtr buffer_limiter, int64_t limit)
        : ScanOperatorFactory(id, scan_node),
          _chunk_buffer(BalanceStrategy::kDirect, dop, std::move(buffer_limiter)),
          _column_ids(std::move(column_ids)),
          _expr_ctxs(std::move(expr_ctxs)),
          _type_is_nullable(std::move(type_is_nullable)),
          _common_sub_column_ids(std::move(common_sub_column_ids)),
          _common_sub_expr_ctxs(std::move(common_sub_expr_ctxs)),
          _is_ai_expr(std::move(is_ai_expr)),
          _ai_model_configs(std::move(ai_model_configs)) {
    _ctx = std::make_shared<AIScanContext>(_chunk_buffer, std::move(ai_chunk_buffer), _column_ids, _expr_ctxs,
                                           _type_is_nullable, _common_sub_column_ids, _common_sub_expr_ctxs,
                                           _is_ai_expr, limit);
}

Status AIScanOperatorFactory::do_prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_common_sub_expr_ctxs, state));

    DictOptimizeParser::set_output_slot_id(&_common_sub_expr_ctxs, _common_sub_column_ids);
    DictOptimizeParser::set_output_slot_id(&_expr_ctxs, _column_ids);

    RETURN_IF_ERROR(Expr::open(_common_sub_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));

    DCHECK_EQ(_is_ai_expr.size(), _expr_ctxs.size());
    for (size_t i = 0; i < _expr_ctxs.size(); ++i) {
        if (!_is_ai_expr[i]) continue;
        auto* ai_expr = down_cast<AIFunctionCallExpr*>(_expr_ctxs[i]->root());
        const auto& config_id = ai_expr->ai_model_config_id();
        auto it = _ai_model_configs.find(config_id);
        if (it != _ai_model_configs.end()) {
            FunctionContext* fn_ctx = _expr_ctxs[i]->fn_context(ai_expr->fn_context_index());
            fn_ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, it->second.get());
        }
    }

    return Status::OK();
}

void AIScanOperatorFactory::do_close(RuntimeState* state) {
    Expr::close(_expr_ctxs, state);
    Expr::close(_common_sub_expr_ctxs, state);
}

OperatorPtr AIScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<AIScanOperator>(this, _id, driver_sequence, dop, _scan_node, _ctx);
}

// ==================== AIScanOperator ====================

AIScanOperator::AIScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                               ScanNode* scan_node, AIScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, dop, scan_node), _ctx(std::move(ctx)) {}

AIScanOperator::~AIScanOperator() = default;

Status AIScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void AIScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr AIScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<AIChunkSource>(this, _chunk_source_profiles[chunk_source_index].get(), std::move(morsel),
                                           _ctx->get_chunk_buffer(), _ctx->column_ids(), _ctx->expr_ctxs(),
                                           _ctx->type_is_nullable(), _ctx->common_sub_column_ids(),
                                           _ctx->common_sub_expr_ctxs(), _ctx->is_ai_expr(), _ctx->limit());
}

// ---------------------------------------------------------------------------
// Bthread bridge scheduling for AI scan.
//
// AI HTTP dispatch takes 2-30s per sub-chunk. Instead of blocking a
// ScanExecutor pthread or polling in a YieldContext loop, we offload
// the blocking dispatch() call to a bthread where bthread::CV::wait_for()
// yields the underlying pthread.
//
// Flow:
//   pthread (SETUP)  → start bthread → pthread freed immediately
//   bthread          → execute_for_operator() → dispatch() → bthread::CV wait
//                      (pthread yielded to bthread pool during HTTP wait)
//   bthread done     → force_submit(COLLECT task) → bthread exits
//   pthread (COLLECT)→ put result chunk into buffer → finish_task
// ---------------------------------------------------------------------------

// State shared between the bthread and the COLLECT task on pthread.
struct AIBridgeState {
    Status status;
    ChunkPtr result_chunk;
    int64_t scan_rows = 0;
    int64_t scan_bytes = 0;
    int64_t eval_ns = 0;
};

Status AIScanOperator::_trigger_next_scan(RuntimeState* state, int chunk_source_index) {
    ChunkBufferTokenPtr buffer_token;
    if (buffer_token = pin_chunk(1); buffer_token == nullptr) {
        return Status::OK();
    }

    COUNTER_UPDATE(_submit_task_counter, 1);
    _chunk_sources[chunk_source_index]->pin_chunk_token(std::move(buffer_token));
    _num_running_io_tasks++;
    _is_io_task_running[chunk_source_index] = true;

    int32_t driver_id = CurrentThread::current().get_driver_id();

    // Capture values needed by the bthread and COLLECT task.
    auto bridge = std::make_shared<AIBridgeState>();
    auto chunk_source = _chunk_sources[chunk_source_index]; // shared_ptr copy
    auto* scan_executor = this->scan_executor();
    auto wp = this->query_ctx();

    const auto io_task_start_nano = MonotonicNanos();

    // --- SETUP task: prepare input + start bthread, then finish ---
    workgroup::ScanTask setup_task;
    setup_task.workgroup = this->workgroup();
    setup_task.priority = OlapScanNode::compute_priority(_submit_task_counter->value());
    setup_task.task_group = down_cast<const ScanOperatorFactory*>(_factory)->scan_task_group();
    setup_task.peak_scan_task_queue_size_counter = this->peak_scan_task_queue_size_counter();
    // total_yield_point_cnt = 0 means is_finished() returns true after run().
    // The setup task completes immediately — the bthread carries on asynchronously.

    setup_task.work_function = [this, state, chunk_source_index, driver_id,
                                io_task_start_nano, bridge, chunk_source, scan_executor,
                                wp](workgroup::YieldContext& ctx) {
        // Notify the pipeline driver when this task finishes so it can
        // pick up chunks or observe state changes (e.g. source finished).
        // This matches the pattern in ScanOperator::_trigger_next_scan.
        auto notify = scan_defer_notify(this);

        auto sp = wp.lock();
        if (!sp) {
            // Query gone — clean up.
            bridge->status = Status::Cancelled("Query context destroyed");
            _set_scan_status(bridge->status);
            int64_t delta_cpu = chunk_source->get_cpu_time_spent();
            _finish_chunk_source_task(state, chunk_source_index, delta_cpu,
                                     chunk_source->get_scan_rows(), chunk_source->get_scan_bytes());
            ctx.set_finished();
            return;
        }

        SCOPED_SET_TRACE_INFO(driver_id, state->query_id(), state->fragment_instance_id());
        // SETUP runs on a pthread (ScanExecutor), so use the real instance
        // mem tracker.
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());

        COUNTER_UPDATE(chunk_source->io_task_wait_timer(), MonotonicNanos() - io_task_start_nano);

        // Prepare input chunk from morsel (fast, ~microseconds).
        auto* ai_source = down_cast<AIChunkSource*>(chunk_source.get());
        auto start_status = chunk_source->start(state);
        if (!start_status.ok()) {
            bridge->status = start_status;
            _set_scan_status(start_status);
            int64_t delta_cpu = chunk_source->get_cpu_time_spent();
            _finish_chunk_source_task(state, chunk_source_index, delta_cpu,
                                     chunk_source->get_scan_rows(), chunk_source->get_scan_bytes());
            ctx.set_finished();
            return;
        }

        auto prepare_status = ai_source->prepare_input(state);
        if (!prepare_status.ok()) {
            // EndOfFile is normal (morsel consumed or limit reached).
            if (!prepare_status.is_end_of_file()) {
                _set_scan_status(prepare_status);
            }
            int64_t delta_cpu = chunk_source->get_cpu_time_spent();
            _finish_chunk_source_task(state, chunk_source_index, delta_cpu,
                                     chunk_source->get_scan_rows(), chunk_source->get_scan_bytes());
            ctx.set_finished();
            return;
        }

        // Capture task metadata for the COLLECT task that bthread will submit.
        auto wg = this->workgroup();
        auto tg = down_cast<const ScanOperatorFactory*>(_factory)->scan_task_group();
        auto peak_counter = this->peak_scan_task_queue_size_counter();

        // Spawn detached bthread for AI HTTP dispatch. The bthread owns its
        // lifetime: it runs dispatch(), then submits a COLLECT task back to the
        // ScanExecutor pthread and exits. All shared state is captured by value
        // or via shared_ptr (bridge, wp). No join needed — COLLECT task handles
        // all cleanup on the pthread side.
        auto bthread_result = bthreads::start_bthread([this, state, chunk_source_index, ai_source,
                                                        bridge, scan_executor, wp, wg, tg, peak_counter,
                                                        driver_id]() {
            auto sp2 = wp.lock();
            if (!sp2) {
                bridge->status = Status::Cancelled("Query context destroyed during bthread");
                // Submit COLLECT task to do cleanup on pthread.
                workgroup::ScanTask collect_task;
                collect_task.workgroup = wg;
                collect_task.priority = std::numeric_limits<int>::max();
                collect_task.task_group = tg;
                collect_task.peak_scan_task_queue_size_counter = peak_counter;
                collect_task.work_function = [this, state, chunk_source_index, bridge](workgroup::YieldContext& c) {
                    auto notify = scan_defer_notify(this);
                    _set_scan_status(bridge->status);
                    auto& cs = _chunk_sources[chunk_source_index];
                    _finish_chunk_source_task(state, chunk_source_index, cs->get_cpu_time_spent(),
                                             cs->get_scan_rows(), cs->get_scan_bytes());
                    c.set_finished();
                };
                scan_executor->force_submit(std::move(collect_task));
                return;
            }

            // Run synchronous AI evaluation on bthread.
            // dispatch() uses bthread::CV internally — the pthread is yielded
            // during HTTP wait, not blocked.
            {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
                bridge->status = ai_source->evaluate_sync(state, &bridge->result_chunk);
                bridge->scan_rows = ai_source->get_scan_rows();
                bridge->scan_bytes = ai_source->get_scan_bytes();
                bridge->eval_ns = ai_source->last_eval_ns();
            }

            // Submit COLLECT task back to ScanExecutor pthread.
            // High priority so the ready result is drained before new SETUP tasks.
            workgroup::ScanTask collect_task;
            collect_task.workgroup = wg;
            collect_task.priority = std::numeric_limits<int>::max();
            collect_task.task_group = tg;
            collect_task.peak_scan_task_queue_size_counter = peak_counter;
            collect_task.work_function = [this, state, chunk_source_index, bridge,
                                          driver_id](workgroup::YieldContext& c) {
                auto notify = scan_defer_notify(this);

                auto sp3 = query_ctx().lock();
                if (!sp3) {
                    auto& cs = _chunk_sources[chunk_source_index];
                    _finish_chunk_source_task(state, chunk_source_index, cs->get_cpu_time_spent(),
                                             cs->get_scan_rows(), cs->get_scan_bytes());
                    c.set_finished();
                    return;
                }

                SCOPED_SET_TRACE_INFO(driver_id, state->query_id(), state->fragment_instance_id());
                // COLLECT runs on a pthread (not bthread), so use the real
                // instance mem tracker rather than the nullptr override.
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());

                auto& cs = _chunk_sources[chunk_source_index];

                if (!bridge->status.ok()) {
                    if (!bridge->status.is_end_of_file()) {
                        LOG(ERROR) << "AI scan error fragment=" << print_id(state->fragment_instance_id())
                                   << " driver=" << get_driver_sequence() << " error=" << bridge->status.to_string();
                        _set_scan_status(bridge->status);
                    }
                } else if (bridge->result_chunk && bridge->result_chunk->num_rows() > 0) {
                    auto* ai_source_ptr = down_cast<AIChunkSource*>(cs.get());
                    ai_source_ptr->put_chunk_to_buffer(std::move(bridge->result_chunk));
                }

                COUNTER_UPDATE(cs->io_task_exec_timer(), bridge->eval_ns);
                COUNTER_SET(cs->scan_timer(),
                            cs->io_task_wait_timer()->value() + cs->io_task_exec_timer()->value());

                int64_t delta_cpu = cs->get_cpu_time_spent();
                _finish_chunk_source_task(state, chunk_source_index, delta_cpu,
                                         bridge->scan_rows, bridge->scan_bytes);
                c.set_finished();
            };
            scan_executor->force_submit(std::move(collect_task));
        });

        if (!bthread_result.ok()) {
            // Bthread creation failed — fallback to sync execution on this pthread.
            LOG(WARNING) << "[AI] bthread start failed, falling back to sync: " << bthread_result.status().to_string();
            {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
                bridge->status = ai_source->evaluate_sync(state, &bridge->result_chunk);
            }

            if (!bridge->status.ok() && !bridge->status.is_end_of_file()) {
                _set_scan_status(bridge->status);
            } else if (bridge->result_chunk && bridge->result_chunk->num_rows() > 0) {
                ai_source->put_chunk_to_buffer(std::move(bridge->result_chunk));
            }

            COUNTER_UPDATE(chunk_source->io_task_exec_timer(), ai_source->last_eval_ns());
            COUNTER_SET(chunk_source->scan_timer(),
                        chunk_source->io_task_wait_timer()->value() + chunk_source->io_task_exec_timer()->value());

            int64_t delta_cpu = chunk_source->get_cpu_time_spent();
            _finish_chunk_source_task(state, chunk_source_index, delta_cpu,
                                     chunk_source->get_scan_rows(), chunk_source->get_scan_bytes());
        }

        ctx.set_finished();
    };

    return _submit_scan_task(std::move(setup_task), chunk_source_index);
}

void AIScanOperator::attach_chunk_source(int32_t source_index) {
    _ctx->attach_shared_input(_driver_sequence, source_index);
}

void AIScanOperator::detach_chunk_source(int32_t source_index) {
    _ctx->detach_shared_input(_driver_sequence, source_index);
}

bool AIScanOperator::has_shared_chunk_source() const {
    return _ctx->has_active_input();
}

BalancedChunkBuffer& AIScanOperator::get_chunk_buffer() const {
    return _ctx->get_chunk_buffer();
}

} // namespace starrocks::pipeline
