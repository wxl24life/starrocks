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

#include "exec/scan_node.h"

namespace starrocks {

// AIScanNode is a lightweight ScanNode that serves as the plan-node anchor
// for AIScanOperator. It doesn't perform actual scanning; the real IO
// is done by AIChunkSource within the ScanExecutor thread pool.
// This node exists solely to satisfy the ScanOperator/ScanOperatorFactory
// interface that requires a ScanNode* for plan_node_id, name, limit, etc.
class AIScanNode final : public ScanNode {
public:
    AIScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ScanNode(pool, tnode, descs) {
        _name = "ai_scan";
    }
    ~AIScanNode() override = default;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override { return Status::OK(); }

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override {
        return Status::NotSupported("AIScanNode::get_next is not supported; use pipeline execution");
    }

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override {
        return {};
    }
};

} // namespace starrocks
