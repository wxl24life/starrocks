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

#include <string>

#include "common/config.h"
#include "exec/pipeline/scan/ai_chunk_buffer.h"
#include "exec/pipeline/scan/morsel.h"

namespace starrocks::pipeline {

// AIMorsel wraps a Chunk from the upstream pipeline as a morsel for the
// downstream AI scan operator. Each AIMorsel carries one Chunk that the
// AIChunkSource will evaluate AI expressions on.
//
// Inherits ScanMorsel (not ScanMorselX) because MorselPtr is
// unique_ptr<ScanMorsel>; a ScanMorselX-derived class cannot be
// implicitly converted to MorselPtr.
class AIMorsel final : public ScanMorsel {
public:
    AIMorsel(int32_t plan_node_id, ChunkPtr chunk)
            : ScanMorsel(plan_node_id, _empty_scan_range()), _chunk(std::move(chunk)) {}
    ~AIMorsel() override = default;

    ChunkPtr& chunk() { return _chunk; }

private:
    static const TScanRange& _empty_scan_range() {
        static const TScanRange kEmpty;
        return kEmpty;
    }
    ChunkPtr _chunk;
};

// AIMorselQueue pulls chunks from AIChunkBuffer and wraps them as AIMorsels.
// It bridges AIChunkBuffer (producer side) and ScanOperator's morsel-based
// consumption model.
//
// empty() semantics: returns true only when ALL upstream data has been consumed
// (upstream EOS + buffer drained). This ensures ScanOperator::is_finished()
// does not terminate prematurely while chunks are still being produced.
class AIMorselQueue final : public MorselQueue {
public:
    AIMorselQueue(int32_t plan_node_id, AIChunkBufferPtr buffer)
            : _plan_node_id(plan_node_id), _buffer(std::move(buffer)) {}
    ~AIMorselQueue() override = default;

    bool empty() const override { return _buffer->is_finished(); }

    StatusOr<MorselPtr> try_get() override {
        ChunkPtr chunk;
        if (_buffer->try_get(&chunk)) {
            return std::make_unique<AIMorsel>(_plan_node_id, std::move(chunk));
        }
        return nullptr;
    }

    std::string name() const override { return "ai_morsel_queue"; }
    Type type() const override { return FIXED; }

    // Return a stable value; the actual morsel count is unknown upfront
    // (chunks arrive dynamically from the upstream pipeline).
    size_t num_original_morsels() const override { return 1; }
    size_t max_degree_of_parallelism() const override {
        return static_cast<size_t>(config::io_tasks_per_scan_operator);
    }

private:
    int32_t _plan_node_id;
    AIChunkBufferPtr _buffer;
};

// AIMorselQueueFactory creates a shared AIMorselQueue for multiple drivers.
class AIMorselQueueFactory final : public MorselQueueFactory {
public:
    AIMorselQueueFactory(int32_t plan_node_id, AIChunkBufferPtr buffer, int dop)
            : _queue(std::make_unique<AIMorselQueue>(plan_node_id, std::move(buffer))),
              _dop(dop) {}
    ~AIMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override { return _queue.get(); }
    size_t size() const override { return _dop; }
    size_t num_original_morsels() const override { return 1; }

    bool is_shared() const override { return true; }
    bool could_local_shuffle() const override { return true; }

private:
    MorselQueuePtr _queue;
    int _dop;
};

} // namespace starrocks::pipeline
