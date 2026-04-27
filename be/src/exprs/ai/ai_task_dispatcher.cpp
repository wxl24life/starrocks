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

#include "exprs/ai/ai_task_dispatcher.h"

#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <butil/fast_rand.h>
#include <butil/time.h>

#include <algorithm>
#include <limits>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/ai/ai_rate_limiter.h"
#include "runtime/current_thread.h"
#include "util/ai_http_client.h"

namespace starrocks {

// ---------------------------------------------------------------------------
// AIDispatchState
// ---------------------------------------------------------------------------

AIDispatchState::~AIDispatchState() {
    for (auto& slot : in_flight) {
        if (slot.ctx != nullptr) {
            slot.ctx->cancel();
            AIRateLimiter::instance()->release_inflight();
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

static constexpr int64_t kRateLimitDeferralUs = 50'000;     // 50ms — deferral when inflight/QPS cap not available
static constexpr int64_t kCvFallbackWaitUs = 100'000;       // 100ms — CV wait when no pending retry cooldown
static constexpr int     kBackoffBaseMs = 1000;             // 1s — initial retry backoff
static constexpr int     kBackoffMaxMs = 32'000;            // 32s — maximum retry backoff
static constexpr int     kBackoffMaxShift = 5;              // caps exponential growth at 2^5 = 32x

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static bool is_retryable_transport_error(const Status& status) {
    if (status.is_cancelled()) return false;
    return status.is_internal_error() || status.is_resource_busy();
}

// Classification of an AI HTTP response for retry decisions.
// Centralizes all provider-specific retry logic in one place.
enum class AIResponseAction {
    kDone,       // Terminal — pass response to caller
    kRetry,      // Transient error — retry with standard backoff
    kThrottle,   // Rate-limited — retry with extended backoff + notify rate limiter
};

// Check response body (case-insensitive) for throttle indicators.
// DashScope may embed throttle messages in 400/500/503 responses.
static bool body_indicates_throttle(const std::string& body) {
    if (body.empty()) return false;
    std::string lower(body.size(), '\0');
    std::transform(body.begin(), body.end(), lower.begin(), ::tolower);
    return lower.find("throttl") != std::string::npos ||
           lower.find("rate limit") != std::string::npos ||
           lower.find("rate_limit") != std::string::npos ||
           lower.find("too many") != std::string::npos;
}

static AIResponseAction classify_response(int http_code, const std::string& body) {
    switch (http_code) {
    case 429:
        return AIResponseAction::kThrottle;
    case 408:
    case 502:
    case 504:
        return AIResponseAction::kRetry;
    case 500:
    case 503:
        return body_indicates_throttle(body) ? AIResponseAction::kThrottle : AIResponseAction::kRetry;
    case 400:
        return body_indicates_throttle(body) ? AIResponseAction::kThrottle : AIResponseAction::kDone;
    default:
        return AIResponseAction::kDone;
    }
}

static int64_t backoff_delay_us(int attempt) {
    DCHECK_GE(attempt, 1) << "backoff_delay_us called with attempt=" << attempt;
    int shift = std::min(std::max(attempt - 1, 0), kBackoffMaxShift);
    int ms = std::min(kBackoffBaseMs * (1 << shift), kBackoffMaxMs);
    ms += butil::fast_rand() % (ms / 4 + 1);
    return static_cast<int64_t>(ms) * 1000;
}

// ---------------------------------------------------------------------------
// Shared implementation helpers
// ---------------------------------------------------------------------------

bool AITaskDispatcher::is_cancelled(const AIDispatchState& state) {
    if (state.tasks == nullptr || state.tasks->empty()) return false;
    auto* flag = (*state.tasks)[0].cancelled;
    return flag != nullptr && flag->load(std::memory_order_acquire);
}

Status AITaskDispatcher::fire_one_request(AIDispatchState& state, InFlightSlot& slot) {
    auto& task = (*state.tasks)[slot.task_index];

    if (is_cancelled(state)) {
        return Status::Cancelled("AI dispatch cancelled");
    }

    // Check inflight cap before consuming a QPS token, so a rejected request
    // does not permanently lose the token from the rate limiter bucket.
    if (!AIRateLimiter::instance()->try_acquire_inflight()) {
        task.http_ctx = nullptr;
        return Status::OK();
    }

    if (!AIRateLimiter::instance()->acquire(task.endpoint, task.cancelled)) {
        AIRateLimiter::instance()->release_inflight();
        task.http_ctx = nullptr;
        return Status::OK();
    }

    // Detach TLS mem tracker before brpc async call: brpc internal allocations
    // must not be charged to the query's MemTracker since they outlive the query.
    tls_mem_tracker = nullptr;
    auto result = AiHttpClient::post_async(task.endpoint, task.api_key, task.post_data,
                                           config::ai_function_http_timeout_ms, task.auth_headers,
                                           state.notify_completion);
    if (!result.ok()) {
        AIRateLimiter::instance()->release_inflight();
        LOG(WARNING) << "[AI] phase=dispatch_fire_rpc " << state.diag << "row=" << task.row_index
                     << " error=" << result.status().to_string();
        return result.status();
    }

    slot.ctx = std::move(result.value());
    return Status::OK();
}

bool AITaskDispatcher::handle_fire_failure(AIDispatchState& state, InFlightSlot& slot, const Status& st) {
    auto& task = (*state.tasks)[slot.task_index];
    if (st.ok() && slot.ctx == nullptr) {
        // Inflight cap or QPS token not available — defer rather than drop.
        // The slot stays in in_flight with retry_after_us set so that
        // poll_completions will re-fire it after a short delay.
        ++state.rate_limit_deferrals;
        slot.retry_after_us = butil::gettimeofday_us() + kRateLimitDeferralUs;
        return false;
    }
    if (!st.ok()) {
        if (is_retryable_transport_error(st) && slot.retries < slot.retry_limit) {
            ++slot.retries;
            ++state.http_retry_events;
            slot.retry_after_us = butil::gettimeofday_us() + backoff_delay_us(slot.retries);
            return false;
        }
        task.http_ctx = nullptr;
        LOG(WARNING) << "[AI] phase=dispatch_failed " << state.diag << "row=" << task.row_index
                     << " attempts=" << slot.retries + 1 << " error=" << st.to_string();
        ++state.completed_count;
        return true;
    }
    return false;
}

bool AITaskDispatcher::process_completed_slot(AIDispatchState& state, InFlightSlot& slot) {
    auto& task = (*state.tasks)[slot.task_index];
    int http_code = slot.ctx->http_status_code();

    // Release inflight slot — HTTP round-trip is done regardless of outcome.
    AIRateLimiter::instance()->release_inflight();

    // Classify: transport failure → kRetry, HTTP code/body → kRetry/kThrottle/kDone.
    AIResponseAction action = AIResponseAction::kDone;
    if (http_code == 0 && !slot.ctx->status().ok() && !slot.ctx->is_cancelled()) {
        action = AIResponseAction::kRetry;
    } else if (http_code > 0) {
        action = classify_response(http_code, slot.ctx->response());
    }

    // Execute retry if applicable.
    if (action != AIResponseAction::kDone) {
        int max_retries = (action == AIResponseAction::kThrottle)
                                  ? config::ai_function_max_retries_on_throttle
                                  : config::ai_function_max_retries;
        if (slot.retries < max_retries) {
            if (action == AIResponseAction::kThrottle) {
                AIRateLimiter::instance()->on_rate_limited(task.endpoint);
            }
            ++slot.retries;
            ++state.http_retry_events;
            slot.retry_limit = max_retries;
            slot.retry_after_us = butil::gettimeofday_us() + backoff_delay_us(slot.retries);
            LOG(WARNING) << "[AI] phase=dispatch_retry " << state.diag << "row=" << task.row_index
                         << " HTTP " << http_code << " (" << slot.retries << "/" << max_retries << ")";
            task.http_ctx = nullptr;
            slot.ctx = nullptr;
            return false;
        }
    }

    // Terminal non-error response proves endpoint is reachable — reset 429 backoff.
    // Only kDone responses qualify; kRetry/kThrottle after retry exhaustion do not.
    if (http_code >= 200 && action == AIResponseAction::kDone) {
        AIRateLimiter::instance()->on_success(task.endpoint);
    }

    task.http_ctx = slot.ctx;
    return true;
}

void AITaskDispatcher::log_dispatch_done(const AIDispatchState& state) {
    const int64_t elapsed_ms = (butil::gettimeofday_us() - state.dispatch_start_us) / 1000;
    VLOG(1) << "[AI] phase=dispatch_done " << state.diag << "num_tasks=" << state.tasks->size()
            << " elapsed_ms=" << elapsed_ms << " peak_in_flight=" << state.peak_in_flight
            << " retries=" << state.http_retry_events;
}

// ---------------------------------------------------------------------------
// fire_async: Phase 1 — fire all HTTP requests (non-blocking).
// ---------------------------------------------------------------------------
StatusOr<std::shared_ptr<AIDispatchState>> AITaskDispatcher::fire_async(std::vector<AITask>& tasks) {
    if (tasks.empty()) {
        auto state = std::make_shared<AIDispatchState>();
        state->tasks = &tasks;
        state->completed_count = 0;
        return state;
    }

    tls_mem_tracker = nullptr;

    if (tasks[0].endpoint.empty()) {
        return Status::InvalidArgument(
                "AI function model endpoint is not configured. "
                "Set 'ai_default_model_endpoint' in FE config or provide endpoint via AI model resource.");
    }
    if (tasks[0].api_key.empty()) {
        return Status::InvalidArgument(
                "AI function API key is not configured. "
                "Set 'AI_FUNCTION_MODEL_API_KEY' env var before FE starts, or provide api_key via AI model resource.");
    }
#ifndef NDEBUG
    for (size_t i = 1; i < tasks.size(); ++i) {
        DCHECK_EQ(tasks[i].endpoint, tasks[0].endpoint) << "batch endpoint mismatch at index " << i;
        DCHECK_EQ(tasks[i].api_key, tasks[0].api_key) << "batch api_key mismatch at index " << i;
    }
#endif

    auto state = std::make_shared<AIDispatchState>();
    state->tasks = &tasks;
    state->dispatch_start_us = butil::gettimeofday_us();

    state->diag = "query_id=";
    state->diag += tasks[0].query_id.empty() ? "n/a" : tasks[0].query_id;
    if (!tasks[0].fragment_instance_id.empty()) {
        state->diag += " fragment_instance_id=";
        state->diag += tasks[0].fragment_instance_id;
    }
    state->diag += ' ';

    // Shared CV state for brpc completion callbacks.
    // Use weak_ptr so the callback does not prevent CVState (and its mutex/CV)
    // from being freed when the dispatch is done.
    state->cv_state = std::make_shared<AIDispatchState::CVState>();
    std::weak_ptr<AIDispatchState::CVState> cv_state_weak = state->cv_state;
    state->notify_completion = [cv_state_weak]() {
        tls_mem_tracker = nullptr;
        auto cv = cv_state_weak.lock();
        if (!cv) return;
        std::lock_guard<bthread::Mutex> lk(cv->mu);
        cv->epoch++;
        cv->cv.notify_one();
    };

    state->in_flight.reserve(tasks.size());

    // Fire all tasks (rate-limited by AIRateLimiter)
    for (size_t i = 0; i < tasks.size(); ++i) {
        if (is_cancelled(*state)) {
            return Status::Cancelled("AI dispatch cancelled");
        }

        InFlightSlot slot;
        slot.task_index = i;
        slot.retry_limit = config::ai_function_max_retries;

        Status st = fire_one_request(*state, slot);
        if (handle_fire_failure(*state, slot, st)) {
            continue;
        }
        state->in_flight.push_back(std::move(slot));
    }

    state->peak_in_flight = state->in_flight.size();
    return state;
}

// ---------------------------------------------------------------------------
// poll_completions: Phase 2 — non-blocking check for completions.
// Returns true when all tasks are done.
// ---------------------------------------------------------------------------
StatusOr<bool> AITaskDispatcher::poll_completions(AIDispatchState& state) {
    if (state.all_done()) return true;

    if (is_cancelled(state)) {
        for (auto& slot : state.in_flight) {
            if (slot.ctx != nullptr) {
                slot.ctx->cancel();
                AIRateLimiter::instance()->release_inflight();
            }
        }
        state.in_flight.clear();
        LOG(WARNING) << "[AI] phase=dispatch_cancelled " << state.diag << "completed=" << state.completed_count
                     << " total=" << state.tasks->size();
        return Status::Cancelled("AI dispatch cancelled");
    }

    tls_mem_tracker = nullptr;

    // Re-fire retry slots whose cooldown has elapsed
    int64_t now_us = butil::gettimeofday_us();
    for (auto& slot : state.in_flight) {
        if (slot.ctx == nullptr && !slot.dead && slot.retry_after_us <= now_us) {
            Status st = fire_one_request(state, slot);
            if (st.ok() && slot.ctx != nullptr) {
                continue;
            }
            if (handle_fire_failure(state, slot, st)) {
                slot.dead = true;
            }
        }
    }

    // Collect completed slots — swap-and-pop for O(1) removal.
    for (size_t i = 0; i < state.in_flight.size();) {
        auto& slot = state.in_flight[i];
        bool remove = false;
        if (slot.dead) {
            remove = true;
        } else if (slot.ctx != nullptr && slot.ctx->is_done()) {
            if (process_completed_slot(state, slot)) {
                ++state.completed_count;
                remove = true;
            }
        }
        if (remove) {
            if (i + 1 < state.in_flight.size()) {
                std::swap(state.in_flight[i], state.in_flight.back());
            }
            state.in_flight.pop_back();
        } else {
            ++i;
        }
    }

    state.peak_in_flight = std::max(state.peak_in_flight, state.in_flight.size());
    ++state.poll_cycles;

    if (state.all_done()) {
        log_dispatch_done(state);
        tls_mem_tracker = nullptr;
        return true;
    }

    return false;
}

// ---------------------------------------------------------------------------
// dispatch: Blocking wrapper — fire_async + poll with CV wait.
// Used by non-operator callers (scalar function path).
// ---------------------------------------------------------------------------
Status AITaskDispatcher::dispatch(std::vector<AITask>& tasks) {
    if (tasks.empty()) return Status::OK();

    ASSIGN_OR_RETURN(auto state, fire_async(tasks));

    if (state->all_done()) return Status::OK();

    // Event-driven wait loop using bthread CV (butex-based, yields bthread).
    // Termination is guaranteed by:
    //   1. Each HTTP request has brpc timeout (ai_function_http_timeout_ms)
    //   2. Retries are bounded (ai_function_max_retries / max_retries_on_throttle)
    //   3. query_timeout triggers cancellation via RuntimeState::_is_cancelled,
    //      detected by is_cancelled() in poll_completions()
    while (true) {
        // Snapshot epoch before checking completions
        // Detach TLS mem tracker before bthread mutex/CV operations.
        tls_mem_tracker = nullptr;

        int64_t epoch_snapshot;
        {
            std::lock_guard<bthread::Mutex> lk(state->cv_state->mu);
            epoch_snapshot = state->cv_state->epoch;
        }

        ASSIGN_OR_RETURN(bool done, poll_completions(*state));
        if (done) return Status::OK();

        // Compute wait deadline from earliest retry cooldown
        int64_t earliest_retry_us = std::numeric_limits<int64_t>::max();
        for (const auto& slot : state->in_flight) {
            if (slot.ctx == nullptr && !slot.dead) {
                earliest_retry_us = std::min(earliest_retry_us, slot.retry_after_us);
            }
        }

        {
            std::unique_lock<bthread::Mutex> lk(state->cv_state->mu);
            if (state->cv_state->epoch != epoch_snapshot) {
                continue;
            }
            if (earliest_retry_us < std::numeric_limits<int64_t>::max()) {
                int64_t wait_us = std::max<int64_t>(1, earliest_retry_us - butil::gettimeofday_us());
                state->cv_state->cv.wait_for(lk, wait_us);
            } else {
                state->cv_state->cv.wait_for(lk, kCvFallbackWaitUs);
            }
        }
    }
}

} // namespace starrocks
