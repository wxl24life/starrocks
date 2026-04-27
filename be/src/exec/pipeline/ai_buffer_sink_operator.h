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

#include "common/config.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/ai_chunk_buffer.h"

namespace starrocks::pipeline {

// AIBufferSinkOperator is the terminal operator of the upstream pipeline.
// It receives chunks from its preceding operator and pushes them into the
// shared AIChunkBuffer. When finishing, it calls mark_sink_finished() to
// decrement the reference count; EOS is only set after ALL drivers finish.
//
// Backpressure: need_input() returns false when the buffer is full, causing
// the driver to enter OUTPUT_FULL state and yield the pipeline thread.
// PipelineDriverPoller will re-check periodically and resume the driver
// once the downstream AI pipeline consumes chunks and frees buffer space.
//
// Sub-chunk splitting: large chunks are split into sub-chunks of
// config::ai_function_sub_chunk_size rows. If the buffer fills up mid-split,
// the remaining data is held in _pending_chunk/_pending_offset and
// drained on subsequent need_input() calls.
class AIBufferSinkOperator final : public Operator {
public:
    static size_t sub_chunk_size() { return std::max<int32_t>(1, config::ai_function_sub_chunk_size); }

    AIBufferSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                         AIChunkBufferPtr buffer, std::string diag_tag);
    ~AIBufferSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    // Push sub-chunks from _pending_chunk into the buffer until the buffer
    // is full or all pending data is consumed.  Const because the base-class
    // need_input() is pure-virtual const; pending state uses mutable members.
    void _drain_pending() const;

    AIChunkBufferPtr _buffer;
    const std::string _diag_tag;
    bool _is_finished = false;
    // Pending sub-chunk state: when a large chunk is partially pushed
    // (buffer became full mid-split), the remainder is held here and
    // drained on subsequent need_input() calls.  Mutable because
    // Operator::need_input() is pure-virtual const, but draining pending
    // data is a necessary side-effect (like flushing an internal buffer).
    mutable ChunkPtr _pending_chunk;
    mutable size_t _pending_offset{0};
    mutable int64_t _last_backpressure_log_us = 0;
    size_t _total_rows_pushed{0};
    const size_t _sub_chunk_size{static_cast<size_t>(std::max<int32_t>(1, config::ai_function_sub_chunk_size))};
};

class AIBufferSinkOperatorFactory final : public OperatorFactory {
public:
    AIBufferSinkOperatorFactory(int32_t id, int32_t plan_node_id, AIChunkBufferPtr buffer, std::string diag_tag);
    ~AIBufferSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    AIChunkBufferPtr _buffer;
    const std::string _diag_tag;
};

} // namespace starrocks::pipeline
