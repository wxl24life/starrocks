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

#include <bthread/mutex.h>

#include <atomic>
#include <queue>

#include "column/chunk.h"

namespace starrocks::pipeline {

// Thread-safe producer-consumer chunk queue bridging the upstream pipeline
// (which produces raw chunks) and the downstream AI scan pipeline
// (which consumes chunks, evaluates AI expressions, and outputs results).
//
// Supports multi-producer (upstream DOP > 1) via reference-counted EOS:
// each upstream driver calls mark_sink_finished() when done; the buffer
// only transitions to EOS after ALL producers have finished.
//
// Provides dual-dimension backpressure: is_full() returns true when the
// buffer reaches capacity OR memory limit, causing
// AIBufferSinkOperator::need_input() to return false and throttle the
// upstream pipeline (OUTPUT_FULL state).  This follows the same pattern
// as ChunkBufferMemoryManager in LocalExchange.
//
// Memory accounting follows BalancedChunkBuffer's convention: the buffer
// tracks _memory_usage locally for statistics and is_full() checks, but
// does NOT call MemTracker::consume/release. Actual memory tracking is
// handled by the thread-local MemTracker (tls_mem_tracker) via malloc
// hooks — the pipeline driver thread that calls put() already has its
// tls_mem_tracker set to instance_mem_tracker by the framework.
class AIChunkBuffer {
public:
    explicit AIChunkBuffer(size_t capacity = kDefaultCapacity, int num_sinks = 1, int64_t mem_limit = 0)
            : _capacity(capacity), _num_remaining_sinks(num_sinks), _mem_limit(mem_limit) {}
    ~AIChunkBuffer() = default;

    static constexpr size_t kDefaultCapacity = 256;

    void put(ChunkPtr chunk) {
        int64_t mem = chunk ? chunk->memory_usage() : 0;
        std::lock_guard<bthread::Mutex> lock(_mutex);
        _memory_usage += mem;
        _chunks.push(std::move(chunk));
    }

    bool try_get(ChunkPtr* chunk) {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        if (_chunks.empty()) {
            return false;
        }
        *chunk = std::move(_chunks.front());
        _chunks.pop();
        int64_t mem = (*chunk) ? (*chunk)->memory_usage() : 0;
        _memory_usage -= mem;
        return true;
    }

    void mark_sink_finished() {
        int remaining = _num_remaining_sinks.fetch_sub(1, std::memory_order_acq_rel);
        if (remaining <= 1) {
            _eos.store(true, std::memory_order_release);
        }
    }

    bool is_eos() const { return _eos.load(std::memory_order_acquire); }

    bool is_finished() const { return is_eos() && empty(); }

    bool is_full() const {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        if (_chunks.size() >= _capacity) {
            return true;
        }
        if (_mem_limit > 0 && _memory_usage >= _mem_limit) {
            return true;
        }
        return false;
    }

    bool empty() const {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        return _chunks.empty();
    }

    size_t size() const {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        return _chunks.size();
    }

    int64_t memory_usage() const {
        std::lock_guard<bthread::Mutex> lock(_mutex);
        return _memory_usage;
    }

    size_t capacity() const { return _capacity; }
    int64_t mem_limit() const { return _mem_limit; }

private:
    mutable bthread::Mutex _mutex;
    std::queue<ChunkPtr> _chunks;
    size_t _capacity;
    int64_t _memory_usage{0};
    std::atomic<int> _num_remaining_sinks;
    int64_t _mem_limit{0};
    std::atomic<bool> _eos{false};
};

using AIChunkBufferPtr = std::shared_ptr<AIChunkBuffer>;

} // namespace starrocks::pipeline
