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

#include "exec/ai_scan_node.h"
#include "exec/exec_node.h"
#include "exprs/ai/ai_default_configuration.h"
#include "exprs/expr_context.h"

namespace starrocks {

// ExecNode for AI_PROJECT_NODE. During pipeline decomposition, it splits
// into two pipelines:
//   Pipeline 1 (upstream):  [child operators] → AIBufferSinkOperator
//   Pipeline 2 (downstream): AIScanOperator → [limit]
// The two pipelines are connected through a shared AIChunkBuffer.
class AIProjectNode final : public ExecNode {
public:
    AIProjectNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc);
    ~AIProjectNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<SlotId> _slot_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;

    std::vector<SlotId> _common_sub_slot_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;

    AIModelConfigMap _ai_model_configs;

    const DescriptorTbl& _desc_tbl;
    // Owned lightweight ScanNode for AIScanOperator's plan-node anchor
    std::unique_ptr<AIScanNode> _ai_scan_node;
};

} // namespace starrocks
