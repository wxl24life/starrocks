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

#include "exec/pipeline/scan/ai_chunk_source.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/pipeline/scan/ai_morsel_queue.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exprs/ai_function_call_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/time.h"

namespace starrocks::pipeline {

AIChunkSource::AIChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                             BalancedChunkBuffer& chunk_buffer, const std::vector<int32_t>& column_ids,
                             const std::vector<ExprContext*>& expr_ctxs, const std::vector<bool>& type_is_nullable,
                             const std::vector<int32_t>& common_sub_column_ids,
                             const std::vector<ExprContext*>& common_sub_expr_ctxs,
                             const std::vector<bool>& is_ai_expr, int64_t limit)
        : ChunkSource(op, runtime_profile, std::move(morsel), chunk_buffer),
          _column_ids(column_ids),
          _src_expr_ctxs(expr_ctxs),
          _type_is_nullable(type_is_nullable),
          _common_sub_column_ids(common_sub_column_ids),
          _src_common_sub_expr_ctxs(common_sub_expr_ctxs),
          _is_ai_expr(is_ai_expr),
          _limit(limit) {}

Status AIChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ChunkSource::prepare(state));
    RETURN_IF_ERROR(Expr::clone_if_not_exists(state, &_obj_pool, _src_expr_ctxs, &_own_expr_ctxs));
    RETURN_IF_ERROR(
            Expr::clone_if_not_exists(state, &_obj_pool, _src_common_sub_expr_ctxs, &_own_common_sub_expr_ctxs));

    _ai_eval_timer = ADD_CHILD_TIMER(_runtime_profile, "AIEvalTime", "IOTaskExecTime");
    return Status::OK();
}

void AIChunkSource::close(RuntimeState* state) {
    Expr::close(_own_expr_ctxs, state);
    Expr::close(_own_common_sub_expr_ctxs, state);
}

// ---------------------------------------------------------------------------
// Input preparation — extracts chunk from morsel, applies limit trimming.
// ---------------------------------------------------------------------------
Status AIChunkSource::prepare_input(RuntimeState* state) {
    if (state->is_cancelled()) {
        _status = Status::Cancelled("Query cancelled");
        return _status;
    }

    if (_morsel_consumed || _reach_eof()) {
        _reach_limit.store(true);
        _status = Status::EndOfFile("AI chunk source finished");
        return _status;
    }

    auto* ai_morsel = down_cast<AIMorsel*>(_morsel.get());
    _input_chunk = std::move(ai_morsel->chunk());

    if (_input_chunk == nullptr || _input_chunk->is_empty()) {
        _input_chunk.reset();
        _morsel_consumed = true;
        _reach_limit.store(true);
        _status = Status::EndOfFile("Empty input chunk");
        return _status;
    }

    // Trim input chunk when approaching the limit.
    if (_limit > 0 && _chunk_rows_read + static_cast<int64_t>(_input_chunk->num_rows()) > _limit) {
        int64_t rows_needed = _limit - _chunk_rows_read;
        if (rows_needed <= 0) {
            _input_chunk.reset();
            _morsel_consumed = true;
            _reach_limit.store(true);
            _status = Status::EndOfFile("AI chunk source limit reached");
            return _status;
        }
        ChunkPtr trimmed = _input_chunk->clone_empty(rows_needed);
        trimmed->append(*_input_chunk, 0, static_cast<size_t>(rows_needed));
        _input_chunk = std::move(trimmed);
    }

    return Status::OK();
}

// ---------------------------------------------------------------------------
// Expression evaluation — shared by both sync and bthread paths.
// ---------------------------------------------------------------------------
Status AIChunkSource::_evaluate_expressions(RuntimeState* state, ChunkPtr* output) {
    auto eval_start = MonotonicNanos();

    _last_input_bytes = _input_chunk ? static_cast<int64_t>(_input_chunk->memory_usage()) : 0;

    for (size_t i = 0; i < _common_sub_column_ids.size(); ++i) {
        if (state->is_cancelled()) return Status::Cancelled("Query cancelled during AI evaluation");
        ASSIGN_OR_RETURN(auto col, _own_common_sub_expr_ctxs[i]->evaluate(_input_chunk.get()));
        _input_chunk->append_column(std::move(col), _common_sub_column_ids[i]);
    }

    Columns result_columns(_column_ids.size());
    for (size_t i = 0; i < _column_ids.size(); ++i) {
        if (state->is_cancelled()) return Status::Cancelled("Query cancelled during AI evaluation");

        // Re-install query mem_tracker before each expression.
        // dispatch() clears tls_mem_tracker for bthread safety; this ensures
        // column allocations between dispatches are properly tracked.
        tls_thread_status.set_mem_tracker(state->instance_mem_tracker());

        ColumnPtr col;
        const TypeDescriptor* col_type;
        if (_is_ai_expr[i]) {
            auto* ai_expr = down_cast<AIFunctionCallExpr*>(_own_expr_ctxs[i]->root());
            ASSIGN_OR_RETURN(col, ai_expr->execute_for_operator(_own_expr_ctxs[i], _input_chunk.get()));
            col_type = &ai_expr->type();
        } else {
            ASSIGN_OR_RETURN(col, _own_expr_ctxs[i]->evaluate(_input_chunk.get()));
            col_type = &_own_expr_ctxs[i]->root()->type();
        }

        // Materialize only_null / constant columns.
        if (col->only_null()) {
            col = ColumnHelper::create_column(*col_type, true);
            col->append_nulls(_input_chunk->num_rows());
        } else if (col->is_constant()) {
            ColumnPtr new_col = ColumnHelper::create_column(*col_type, false);
            new_col->append(*down_cast<ConstColumn*>(col.get())->data_column(), 0, 1);
            new_col->assign(_input_chunk->num_rows(), 0);
            col = std::move(new_col);
        }
        if (_type_is_nullable[i] && !col->is_nullable()) {
            col = NullableColumn::create(col, NullColumn::create(col->size(), 0));
        }
        result_columns[i] = std::move(col);
    }

    auto result_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _column_ids.size(); ++i) {
        result_chunk->append_column(result_columns[i], _column_ids[i]);
    }
    result_chunk->owner_info() = _input_chunk->owner_info();
    _input_chunk.reset();
    _last_eval_ns = MonotonicNanos() - eval_start;

    *output = std::move(result_chunk);
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Bthread bridge API
// ---------------------------------------------------------------------------

void AIChunkSource::_finalize_eval(RuntimeState* state) {
    _morsel_consumed = true;
    COUNTER_UPDATE(_ai_eval_timer, _last_eval_ns);
    if (_reach_eof()) {
        _reach_limit.store(true);
    }
}

Status AIChunkSource::evaluate_sync(RuntimeState* state, ChunkPtr* output) {
    ChunkPtr result;
    RETURN_IF_ERROR(_evaluate_expressions(state, &result));

    _scan_rows_num += result->num_rows();
    _scan_bytes += _last_input_bytes;
    _chunk_rows_read += result->num_rows();
    _finalize_eval(state);

    // Mark source as finished so has_next_chunk() returns false.
    _status = Status::EndOfFile("AI chunk source morsel consumed");
    *output = std::move(result);
    return Status::OK();
}

void AIChunkSource::put_chunk_to_buffer(ChunkPtr chunk) {
    // owner_id is always 0 for AIMorsel (empty TScanRange) — all chunks go
    // to lane 0 in BalancedChunkBuffer, which is correct for shared morsel mode.
    auto [owner_id, version] = _morsel->get_lane_owner_and_version();
    chunk->owner_info().set_owner_id(owner_id, true);
    _chunk_buffer.put(_scan_operator_seq, std::move(chunk), std::move(_chunk_token));
}

// ---------------------------------------------------------------------------
// Pure-virtual override required by ChunkSource. Not called in normal AI
// execution (AIScanOperator overrides _trigger_next_scan with a bthread
// bridge that calls prepare_input/evaluate_sync directly). Retained as the
// mandatory interface implementation and potential future fallback path.
// ---------------------------------------------------------------------------
Status AIChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
    RETURN_IF_ERROR(prepare_input(state));

    ChunkPtr result;
    RETURN_IF_ERROR(_evaluate_expressions(state, &result));

    _scan_rows_num += result->num_rows();
    _scan_bytes += _last_input_bytes;
    _chunk_rows_read += result->num_rows();
    *chunk = std::move(result);
    _chunk_buffer.update_limiter(chunk->get());
    _finalize_eval(state);
    return Status::OK();
}

} // namespace starrocks::pipeline
