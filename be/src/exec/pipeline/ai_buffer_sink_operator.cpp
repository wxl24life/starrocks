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

#include "exec/pipeline/ai_buffer_sink_operator.h"

#include <butil/time.h>

#include "common/logging.h"

namespace starrocks::pipeline {

// ---- AIBufferSinkOperator ----

AIBufferSinkOperator::AIBufferSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           int32_t driver_sequence, AIChunkBufferPtr buffer, std::string diag_tag)
        : Operator(factory, id, "ai_buffer_sink", plan_node_id, false, driver_sequence),
          _buffer(std::move(buffer)),
          _diag_tag(std::move(diag_tag)) {}

bool AIBufferSinkOperator::need_input() const {
    if (_is_finished) return false;
    if (_pending_chunk != nullptr) {
        _drain_pending();
        if (_pending_chunk != nullptr) {
            return false;
        }
    }
    if (_buffer->is_full()) {
        const int64_t now_us = butil::gettimeofday_us();
        if (now_us - _last_backpressure_log_us >= 5'000'000) {
            _last_backpressure_log_us = now_us;
            LOG(INFO) << "[AI] phase=buffer_sink_backpressure " << _diag_tag << "driver=" << _driver_sequence
                      << " buffer_size=" << _buffer->size() << " capacity=" << _buffer->capacity();
        }
        return false;
    }
    return true;
}

Status AIBufferSinkOperator::set_finishing(RuntimeState* state) {
    // Flush any remaining pending sub-chunks before finishing.
    // The buffer capacity limit is relaxed here — we must not lose data.
    if (_pending_chunk != nullptr) {
        const size_t scs = _sub_chunk_size;
        const size_t num_rows = _pending_chunk->num_rows();
        while (_pending_offset < num_rows) {
            size_t rows = std::min(scs, num_rows - _pending_offset);
            ChunkPtr sub_chunk = _pending_chunk->clone_empty(rows);
            sub_chunk->append(*_pending_chunk, _pending_offset, rows);
            _buffer->put(std::move(sub_chunk));
            _pending_offset += rows;
        }
        _pending_chunk.reset();
        _pending_offset = 0;
    }
    // Warn if the flush caused buffer memory to exceed the configured limit.
    // This is expected (we must not lose data), but worth logging for capacity tuning.
    if (_buffer->mem_limit() > 0 && _buffer->memory_usage() > _buffer->mem_limit()) {
        LOG(WARNING) << "[AI] phase=buffer_sink_flush_spike " << _diag_tag
                     << "driver=" << _driver_sequence
                     << " buffer_mem=" << _buffer->memory_usage()
                     << " mem_limit=" << _buffer->mem_limit()
                     << " buffer_size=" << _buffer->size();
    }
    _is_finished = true;
    _buffer->mark_sink_finished();
    return Status::OK();
}

StatusOr<ChunkPtr> AIBufferSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from ai_buffer_sink operator");
}

Status AIBufferSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }

    const size_t num_rows = chunk->num_rows();
    const size_t scs = _sub_chunk_size;

    _total_rows_pushed += num_rows;

    if (num_rows <= scs) {
        _buffer->put(chunk);
        return Status::OK();
    }

    // Split and push sub-chunks, stopping when buffer is full.
    // Any remaining data is saved in _pending_chunk/_pending_offset
    // and drained via need_input() → _drain_pending().
    for (size_t offset = 0; offset < num_rows; offset += scs) {
        if (_buffer->is_full()) {
            _pending_chunk = chunk;
            _pending_offset = offset;
            return Status::OK();
        }
        size_t rows = std::min(scs, num_rows - offset);
        ChunkPtr sub_chunk = chunk->clone_empty(rows);
        sub_chunk->append(*chunk, offset, rows);
        _buffer->put(std::move(sub_chunk));
    }
    return Status::OK();
}

void AIBufferSinkOperator::_drain_pending() const {
    if (_pending_chunk == nullptr) return;

    const size_t scs = _sub_chunk_size;
    const size_t num_rows = _pending_chunk->num_rows();

    while (_pending_offset < num_rows && !_buffer->is_full()) {
        size_t rows = std::min(scs, num_rows - _pending_offset);
        ChunkPtr sub_chunk = _pending_chunk->clone_empty(rows);
        sub_chunk->append(*_pending_chunk, _pending_offset, rows);
        _buffer->put(std::move(sub_chunk));
        _pending_offset += rows;
    }

    if (_pending_offset >= num_rows) {
        _pending_chunk.reset();
        _pending_offset = 0;
    }
}

// ---- AIBufferSinkOperatorFactory ----

AIBufferSinkOperatorFactory::AIBufferSinkOperatorFactory(int32_t id, int32_t plan_node_id, AIChunkBufferPtr buffer,
                                                           std::string diag_tag)
        : OperatorFactory(id, "ai_buffer_sink", plan_node_id),
          _buffer(std::move(buffer)),
          _diag_tag(std::move(diag_tag)) {}

OperatorPtr AIBufferSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<AIBufferSinkOperator>(this, _id, _plan_node_id, driver_sequence, _buffer, _diag_tag);
}

} // namespace starrocks::pipeline
