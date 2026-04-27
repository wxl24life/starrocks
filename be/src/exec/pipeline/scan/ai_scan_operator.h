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

#include "exec/pipeline/scan/ai_scan_context.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exprs/ai/ai_default_configuration.h"

namespace starrocks {
class ScanNode;
}

namespace starrocks::pipeline {

class AIScanOperatorFactory final : public ScanOperatorFactory {
public:
    AIScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop, AIChunkBufferPtr ai_chunk_buffer,
                          std::vector<int32_t> column_ids, std::vector<ExprContext*> expr_ctxs,
                          std::vector<bool> type_is_nullable, std::vector<int32_t> common_sub_column_ids,
                          std::vector<ExprContext*> common_sub_expr_ctxs, std::vector<bool> is_ai_expr,
                          AIModelConfigMap ai_model_configs,
                          ChunkBufferLimiterPtr buffer_limiter, int64_t limit = -1);

    ~AIScanOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    BalancedChunkBuffer _chunk_buffer;
    AIScanContextPtr _ctx;

    std::vector<int32_t> _column_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;
    std::vector<int32_t> _common_sub_column_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;
    std::vector<bool> _is_ai_expr;
    AIModelConfigMap _ai_model_configs;
};

class AIScanOperator final : public ScanOperator {
public:
    AIScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop, ScanNode* scan_node,
                   AIScanContextPtr ctx);

    ~AIScanOperator() override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

    workgroup::ScanSchedEntityType sched_entity_type() const override { return workgroup::ScanSchedEntityType::AI; }

    [[nodiscard]] Status _trigger_next_scan(RuntimeState* state, int chunk_source_index) override;

protected:
    void attach_chunk_source(int32_t source_index) override;
    void detach_chunk_source(int32_t source_index) override;
    bool has_shared_chunk_source() const override;
    BalancedChunkBuffer& get_chunk_buffer() const override;

private:
    AIScanContextPtr _ctx;
};

} // namespace starrocks::pipeline
