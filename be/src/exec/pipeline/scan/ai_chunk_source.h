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

#pragma once

#include "common/object_pool.h"
#include "exec/pipeline/scan/ai_morsel_queue.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exprs/ai/ai_default_configuration.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

// AIChunkSource evaluates AI function expressions in the ScanExecutor.
//
// Supports two execution modes:
// 1. Bthread bridge (primary): AIScanOperator calls prepare_input() on
//    pthread, then evaluate_sync() on a bthread where dispatch() uses
//    bthread::CV to yield the pthread during HTTP wait. Results are
//    put into the buffer via put_chunk_to_buffer() on pthread.
// 2. Synchronous fallback: _read_chunk() blocks on the pthread (used
//    if bthread creation fails).
class AIChunkSource final : public ChunkSource {
public:
    AIChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                  BalancedChunkBuffer& chunk_buffer, const std::vector<int32_t>& column_ids,
                  const std::vector<ExprContext*>& expr_ctxs, const std::vector<bool>& type_is_nullable,
                  const std::vector<int32_t>& common_sub_column_ids,
                  const std::vector<ExprContext*>& common_sub_expr_ctxs, const std::vector<bool>& is_ai_expr,
                  int64_t limit);

    ~AIChunkSource() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool reach_limit() override { return _limit != -1 && _reach_limit.load(); }

    // --- Bthread bridge API (called by AIScanOperator) ---

    // Prepare input chunk from morsel. Returns EndOfFile when done.
    // Must be called on pthread before evaluate_sync.
    Status prepare_input(RuntimeState* state);

    // Evaluate all expressions synchronously (including AI HTTP dispatch).
    // Designed to run on a bthread where bthread::CV yields the pthread.
    // On success, writes the result chunk to *output.
    Status evaluate_sync(RuntimeState* state, ChunkPtr* output);

    // Put a completed result chunk into the chunk buffer.
    // Must be called on pthread (ScanExecutor worker).
    void put_chunk_to_buffer(ChunkPtr chunk);

    int64_t last_eval_ns() const { return _last_eval_ns; }

protected:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

private:
    bool _reach_eof() const { return _limit != -1 && _chunk_rows_read >= _limit; }
    Status _evaluate_expressions(RuntimeState* state, ChunkPtr* output);
    void _finalize_eval(RuntimeState* state);

    const std::vector<int32_t>& _column_ids;
    const std::vector<ExprContext*>& _src_expr_ctxs;
    const std::vector<bool>& _type_is_nullable;
    const std::vector<int32_t>& _common_sub_column_ids;
    const std::vector<ExprContext*>& _src_common_sub_expr_ctxs;
    const std::vector<bool>& _is_ai_expr;

    ObjectPool _obj_pool;
    std::vector<ExprContext*> _own_expr_ctxs;
    std::vector<ExprContext*> _own_common_sub_expr_ctxs;

    const int64_t _limit;
    int64_t _chunk_rows_read{0};

    bool _morsel_consumed = false;
    ChunkPtr _input_chunk;
    int64_t _last_input_bytes{0};
    int64_t _last_eval_ns{0};

    // AI-specific profile counter (child of IOTaskExecTime, auto-merged).
    RuntimeProfile::Counter* _ai_eval_timer = nullptr;
};

} // namespace starrocks::pipeline
