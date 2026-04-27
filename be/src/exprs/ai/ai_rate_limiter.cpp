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

#include "exprs/ai/ai_rate_limiter.h"

#include <bthread/bthread.h>
#include <butil/time.h>

#include <algorithm>

#include "common/config.h"
#include "common/logging.h"

namespace starrocks {

// Rate limiter timing constants
static constexpr int64_t kThrottleCheckMaxWaitUs = 100'000;    // 100ms — max wait per throttle check iteration
static constexpr int64_t kTokenWaitMinUs = 100;                // 100us — minimum token refill wait
static constexpr int64_t kTokenWaitMaxUs = 100'000;            // 100ms — maximum token refill wait
static constexpr int64_t kTokenWaitFallbackUs = 10'000;        // 10ms — fallback when refill_rate is 0
static constexpr double  kBurstMultiplier = 2.0;               // burst = 2s worth of tokens
static constexpr int64_t kThrottleBaseBackoffMs = 500;         // 500ms — initial 429 backoff
static constexpr int64_t kThrottleMaxBackoffMs = 30'000;       // 30s — maximum 429 backoff
static constexpr int     kThrottleMaxBackoffShift = 15;        // caps exponential growth at 2^15 ≈ 32k

static int64_t now_us() {
    return butil::gettimeofday_us();
}

AIRateLimiter* AIRateLimiter::instance() {
    static AIRateLimiter limiter;
    return &limiter;
}

void AIRateLimiter::shutdown() {
    _shutdown.store(true, std::memory_order_release);
    // Wake up all waiters so they can observe shutdown and return false.
    std::lock_guard<bthread::Mutex> lock(_map_mutex);
    for (auto& [_, bucket] : _buckets) {
        bucket->cv.notify_all();
    }
}

AIRateLimiter::TokenBucket& AIRateLimiter::get_or_create_bucket(const std::string& endpoint) {
    std::lock_guard<bthread::Mutex> lock(_map_mutex);
    auto it = _buckets.find(endpoint);
    if (it != _buckets.end()) {
        return *it->second;
    }
    auto bucket = std::make_unique<TokenBucket>();
    double qps = static_cast<double>(config::ai_function_rate_limit_qps);
    bucket->refill_rate = std::max(qps, 0.0);
    bucket->max_tokens = std::max(qps, 1.0);
    bucket->tokens = bucket->max_tokens;
    bucket->last_refill_us = now_us();
    auto& ref = *bucket;
    _buckets[endpoint] = std::move(bucket);
    return ref;
}

void AIRateLimiter::refill(TokenBucket& bucket) {
    int64_t now = now_us();
    int64_t elapsed_us = now - bucket.last_refill_us;
    if (elapsed_us <= 0) return;

    // Dynamically pick up config changes — no restart required.
    double configured_qps = static_cast<double>(config::ai_function_rate_limit_qps);
    if (configured_qps > 0) {
        bucket.refill_rate = configured_qps;
        // Burst size = 2 seconds worth of tokens (not full QPS) to avoid
        // DashScope's "Request rate increased too quickly" spike protection.
        bucket.max_tokens = std::max(configured_qps * kBurstMultiplier, 1.0);
    }

    double new_tokens = bucket.refill_rate * (static_cast<double>(elapsed_us) / 1'000'000.0);
    bucket.tokens = std::min(bucket.tokens + new_tokens, bucket.max_tokens);
    bucket.last_refill_us = now;
}

bool AIRateLimiter::try_acquire_one(TokenBucket& bucket) {
    refill(bucket);
    if (bucket.tokens >= 1.0) {
        bucket.tokens -= 1.0;
        return true;
    }
    return false;
}

bool AIRateLimiter::acquire(const std::string& endpoint, const std::atomic<bool>* cancelled) {
    auto& bucket = get_or_create_bucket(endpoint);
    int wait_count = 0;

    while (!_shutdown.load(std::memory_order_acquire)) {
        if (cancelled != nullptr && cancelled->load(std::memory_order_acquire)) {
            return false;
        }
        std::unique_lock<bthread::Mutex> lock(bucket.mu);

        // Check 429 throttle
        if (bucket.throttle_until_us > 0) {
            int64_t now = now_us();
            if (now < bucket.throttle_until_us) {
                int64_t wait_us = std::min(bucket.throttle_until_us - now, kThrottleCheckMaxWaitUs);
                ++wait_count;
                bucket.cv.wait_for(lock, wait_us);
                continue;
            }
            bucket.throttle_until_us = 0;
        }

        // Rate limiting disabled
        if (config::ai_function_rate_limit_qps <= 0) {
            return true;
        }

        // Try to acquire a token
        if (try_acquire_one(bucket)) {
            return true;
        }

        // Compute precise wait time until next token is available.
        // At refill_rate tokens/sec, one token takes 1/refill_rate seconds.
        int64_t wait_us = kTokenWaitFallbackUs;
        if (bucket.refill_rate > 0) {
            double deficit = 1.0 - bucket.tokens;
            wait_us = static_cast<int64_t>(deficit / bucket.refill_rate * 1'000'000.0) + 1;
            wait_us = std::max<int64_t>(wait_us, kTokenWaitMinUs);
            wait_us = std::min<int64_t>(wait_us, kTokenWaitMaxUs);
        }

        ++wait_count;
        bucket.cv.wait_for(lock, wait_us);
    }
    return false;
}

void AIRateLimiter::on_rate_limited(const std::string& endpoint) {
    auto& bucket = get_or_create_bucket(endpoint);
    std::lock_guard<bthread::Mutex> lock(bucket.mu);

    bucket.consecutive_429++;
    // Exponential backoff: 500ms, 1s, 2s, 4s, ... capped at 30s
    int shift = std::min(std::max(bucket.consecutive_429 - 1, 0), kThrottleMaxBackoffShift);
    int64_t backoff_ms = std::min(kThrottleBaseBackoffMs << shift, kThrottleMaxBackoffMs);
    bucket.throttle_until_us = now_us() + backoff_ms * 1000;

    LOG(WARNING) << "[AI] rate_limiter: endpoint " << endpoint << " throttled (429 #" << bucket.consecutive_429
                 << "), backoff " << backoff_ms << "ms";
}

bool AIRateLimiter::try_acquire_inflight() {
    int32_t cap = config::ai_function_max_inflight;
    if (cap <= 0) return true; // disabled
    int64_t cur = _global_in_flight.load(std::memory_order_relaxed);
    while (cur < cap) {
        if (_global_in_flight.compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void AIRateLimiter::release_inflight() {
    DCHECK_GE(_global_in_flight.load(std::memory_order_relaxed), 1) << "inflight counter underflow";
    _global_in_flight.fetch_sub(1, std::memory_order_acq_rel);
}

void AIRateLimiter::on_success(const std::string& endpoint) {
    auto& bucket = get_or_create_bucket(endpoint);
    std::lock_guard<bthread::Mutex> lock(bucket.mu);
    if (bucket.consecutive_429 > 0) {
        bucket.consecutive_429 = 0;
        bucket.throttle_until_us = 0;
        bucket.cv.notify_all();
    }
}

} // namespace starrocks
