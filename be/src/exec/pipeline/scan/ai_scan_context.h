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

#include <mutex>

#include "common/constexpr.h"
#include "exec/pipeline/scan/ai_chunk_buffer.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exprs/expr_context.h"
#include "util/phmap/phmap_fwd_decl.h"

namespace starrocks::pipeline {

class AIScanContext;
using AIScanContextPtr = std::shared_ptr<AIScanContext>;

class AIScanContext {
public:
    using ActiveInputKey = std::pair<int32_t, int32_t>;
    using ActiveInputSet = phmap::parallel_flat_hash_set<ActiveInputKey, typename phmap::Hash<ActiveInputKey>,
                                                         typename phmap::EqualTo<ActiveInputKey>,
                                                         typename std::allocator<ActiveInputKey>, NUM_LOCK_SHARD_LOG,
                                                         std::mutex, true>;

    AIScanContext(BalancedChunkBuffer& chunk_buffer, AIChunkBufferPtr ai_chunk_buffer,
                  std::vector<int32_t> column_ids, std::vector<ExprContext*> expr_ctxs,
                  std::vector<bool> type_is_nullable, std::vector<int32_t> common_sub_column_ids,
                  std::vector<ExprContext*> common_sub_expr_ctxs, std::vector<bool> is_ai_expr,
                  int64_t limit = -1)
            : _chunk_buffer(chunk_buffer),
              _ai_chunk_buffer(std::move(ai_chunk_buffer)),
              _column_ids(std::move(column_ids)),
              _expr_ctxs(std::move(expr_ctxs)),
              _type_is_nullable(std::move(type_is_nullable)),
              _common_sub_column_ids(std::move(common_sub_column_ids)),
              _common_sub_expr_ctxs(std::move(common_sub_expr_ctxs)),
              _is_ai_expr(std::move(is_ai_expr)),
              _limit(limit) {}

    ~AIScanContext() = default;

    BalancedChunkBuffer& get_chunk_buffer() const { return _chunk_buffer; }
    AIChunkBufferPtr ai_chunk_buffer() const { return _ai_chunk_buffer; }

    const std::vector<int32_t>& column_ids() const { return _column_ids; }
    const std::vector<ExprContext*>& expr_ctxs() const { return _expr_ctxs; }
    const std::vector<bool>& type_is_nullable() const { return _type_is_nullable; }
    const std::vector<int32_t>& common_sub_column_ids() const { return _common_sub_column_ids; }
    const std::vector<ExprContext*>& common_sub_expr_ctxs() const { return _common_sub_expr_ctxs; }
    const std::vector<bool>& is_ai_expr() const { return _is_ai_expr; }
    int64_t limit() const { return _limit; }

    void attach_shared_input(int32_t operator_seq, int32_t source_index) {
        auto key = std::make_pair(operator_seq, source_index);
        _active_inputs.emplace(key);
    }

    void detach_shared_input(int32_t operator_seq, int32_t source_index) {
        auto key = std::make_pair(operator_seq, source_index);
        _active_inputs.erase(key);
    }

    bool has_active_input() const { return !_active_inputs.empty(); }

private:
    BalancedChunkBuffer& _chunk_buffer;
    AIChunkBufferPtr _ai_chunk_buffer;

    std::vector<int32_t> _column_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;
    std::vector<int32_t> _common_sub_column_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;
    std::vector<bool> _is_ai_expr;
    int64_t _limit = -1;

    ActiveInputSet _active_inputs;
};

} // namespace starrocks::pipeline
