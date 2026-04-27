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
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

class AiHttpContext;

struct AITask {
    size_t row_index = 0;
    std::string post_data;
    std::string endpoint;
    std::string api_key;
    std::vector<std::pair<std::string, std::string>> auth_headers;
    std::shared_ptr<AiHttpContext> http_ctx;
    const std::atomic<bool>* cancelled = nullptr;
    /// Filled by AiFunctions for log correlation (SLS / multi-fragment diagnosis).
    std::string query_id;
    std::string fragment_instance_id;
};

// Tracks one in-flight HTTP request across yield points.
struct InFlightSlot {
    size_t task_index = 0;
    std::shared_ptr<AiHttpContext> ctx;
    int retries = 0;
    int retry_limit = 0; // dynamic cap: may be raised for 429 (throttle) responses
    int64_t retry_after_us = 0;
    bool dead = false; // terminal failure — slot is awaiting removal
};

// Internal state for the dispatch() fire+poll loop.
struct AIDispatchState {
    std::vector<AITask>* tasks = nullptr; // non-owning pointer to caller's task vector
    std::vector<InFlightSlot> in_flight;
    size_t completed_count = 0;
    int64_t dispatch_start_us = 0;
    std::string diag; // log correlation string

    // Completion notification state (shared with brpc callbacks).
    struct CVState {
        bthread::Mutex mu;
        bthread::ConditionVariable cv;
        int64_t epoch = 0;
    };
    std::shared_ptr<CVState> cv_state;
    std::function<void()> notify_completion; // callback for brpc async

    // Stats
    size_t peak_in_flight = 0;
    size_t rate_limit_deferrals = 0;
    size_t http_retry_events = 0;
    size_t poll_cycles = 0;

    ~AIDispatchState();

    bool all_done() const { return tasks != nullptr && completed_count >= tasks->size(); }
};

// Dispatcher for AI HTTP tasks with rate limiting and retry logic.
//
// dispatch() blocks until all tasks complete. Internally uses bthread::CV
// to yield the bthread during HTTP wait, freeing the underlying pthread.
//
// Request rate is controlled by AIRateLimiter (QPS token bucket).
// brpc handles connection pooling internally.
class AITaskDispatcher {
public:
    // Blocking dispatch: fires all tasks and waits for completion.
    // When called on a bthread, bthread::CV yields the pthread during wait.
    static Status dispatch(std::vector<AITask>& tasks);

private:
    // Internal two-phase implementation used by dispatch().
    static StatusOr<std::shared_ptr<AIDispatchState>> fire_async(std::vector<AITask>& tasks);
    static StatusOr<bool> poll_completions(AIDispatchState& state);

    static Status fire_one_request(AIDispatchState& state, InFlightSlot& slot);
    static bool handle_fire_failure(AIDispatchState& state, InFlightSlot& slot, const Status& st);
    static bool process_completed_slot(AIDispatchState& state, InFlightSlot& slot);
    static bool is_cancelled(const AIDispatchState& state);
    static void log_dispatch_done(const AIDispatchState& state);
};

} // namespace starrocks
