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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

namespace starrocks {

// Per-endpoint adaptive rate limiter for AI HTTP requests — the PRIMARY
// throughput control mechanism.
//
// Design philosophy (aligned with DashScope / Bailian):
//   DashScope enforces server-side rate limits (RPM + TPM + spike protection)
//   per model, per account.  Users can raise these limits via the console.
//   Therefore StarRocks does NOT try to replicate the server-side limits.  Instead:
//
//   1. Token-bucket QPS cap (default 64 QPS via ai_function_rate_limit_qps):
//      proactively limits request rate to avoid overwhelming the LLM endpoint.
//      This is the primary throughput knob.  Set to 0 to disable for testing.
//
//   2. Adaptive 429 backoff: when DashScope returns 429, the endpoint enters
//      an exponential backoff window (500ms → 1s → 2s → ... → 30s).
//      Successful requests reset the backoff counter.
//
//   A global in-flight cap (ai_function_max_inflight) prevents unbounded
//   concurrent HTTP requests when many queries run simultaneously.
//
//   This is similar to Databricks AI Gateway's adaptive approach, and avoids
//   the maintenance burden of per-model QPS/TPM mapping tables.
class AIRateLimiter {
public:
    static AIRateLimiter* instance();

    // Acquire permission to send a request.  Blocks (bthread CV wait) until
    // allowed.  Returns false on shutdown or when *cancelled becomes true.
    bool acquire(const std::string& endpoint, const std::atomic<bool>* cancelled = nullptr);

    // Called when the upstream returns HTTP 429.
    void on_rate_limited(const std::string& endpoint);

    // Called after a successful (non-429) response to reset the backoff.
    void on_success(const std::string& endpoint);

    // Global in-flight HTTP request tracking for yield-mode memory safety.
    // In yield mode, threads are freed quickly, so many queries can fire
    // HTTP concurrently. This cap prevents unbounded in-flight growth.
    bool try_acquire_inflight();
    void release_inflight();
    int64_t current_inflight() const { return _global_in_flight.load(std::memory_order_relaxed); }

    void shutdown();

private:
    AIRateLimiter() = default;

    struct TokenBucket {
        double tokens = 0;
        int64_t last_refill_us = 0;
        double refill_rate = 0;
        double max_tokens = 0;

        // 429 adaptive backoff state
        int64_t throttle_until_us = 0;
        int consecutive_429 = 0;

        bthread::Mutex mu;
        bthread::ConditionVariable cv;  // signalled when tokens may be available
    };

    TokenBucket& get_or_create_bucket(const std::string& endpoint);
    bool try_acquire_one(TokenBucket& bucket);
    void refill(TokenBucket& bucket);

    bthread::Mutex _map_mutex;
    std::unordered_map<std::string, std::unique_ptr<TokenBucket>> _buckets;
    std::atomic<bool> _shutdown{false};
    std::atomic<int64_t> _global_in_flight{0};
};

} // namespace starrocks
