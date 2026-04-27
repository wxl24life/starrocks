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

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "service/brpc.h"

namespace starrocks {

// Context for a single async HTTP request.
//
// Completion is event-driven: when a completion_callback is set (via
// set_completion_callback), it is invoked from the brpc reactor thread
// immediately after set_done(). AITaskDispatcher uses this to signal a
// bthread::ConditionVariable (butex-based), turning the dispatch loop
// from busy-polling into event-driven waiting — analogous to epoll.
// The bthread sleeps until a brpc reactor thread signals that an HTTP
// response has arrived, without occupying any pthread.
class AiHttpContext {
public:
    AiHttpContext() = default;
    ~AiHttpContext() = default;

    bool is_done() const { return _done.load(std::memory_order_acquire); }

    bool is_cancelled() const { return _cancelled.load(std::memory_order_acquire); }

    void cancel();

    const std::string& response() const { return _response; }

    const Status& status() const { return _status; }

    // HTTP status code (200, 429, 500, etc.). Only valid after is_done().
    // 0 means transport-level failure (no HTTP response received).
    int http_status_code() const { return _http_status_code; }

    // Release Controller's internal buffers after the response has been
    // consumed. Reduces jemalloc retained memory during long-running queries.
    void release_resources() {
        _response.clear();
        _response.shrink_to_fit();
        _cntl.Reset();
    }

    // Set a callback that is invoked (from brpc reactor thread) when the
    // request completes. Must be set before post_async() fires the request.
    // The callback must be lightweight (e.g. cv.notify_one()).
    void set_completion_callback(std::function<void()> cb) { _completion_cb = std::move(cb); }

private:
    friend class AiHttpClient;
    friend class AiHttpClosure;

    brpc::Controller _cntl;
    std::string _response;
    Status _status;
    int _http_status_code = 0;
    std::atomic<bool> _done{false};
    std::atomic<bool> _cancelled{false};
    brpc::CallId _call_id;
    std::function<void()> _completion_cb;

    void set_done(const Status& status, std::string response);
};

// Async HTTP client for AI function requests using brpc
// This client sends HTTP requests without blocking the calling thread
class AiHttpClient {
public:
    AiHttpClient() = default;
    ~AiHttpClient() = default;

    // Send an async HTTP POST request.
    // When auth_headers is non-empty, those headers are used instead of the default
    // "Authorization: Bearer <api_key>" header.
    //
    // No concurrency limit — callers may fire as many requests as needed.
    // Rate control is handled by AIRateLimiter (QPS token bucket) upstream.
    // completion_cb, if provided, is set on the AiHttpContext *before* the
    // brpc CallMethod fires, guaranteeing no race between request completion
    // and callback registration (follows the same pattern as StarRocks'
    // BThreadCountDownLatch which is always created before task submission).
    static StatusOr<std::shared_ptr<AiHttpContext>> post_async(
            const std::string& url, const std::string& api_key, const std::string& post_data,
            int32_t timeout_ms = 0,
            const std::vector<std::pair<std::string, std::string>>& auth_headers = {},
            std::function<void()> completion_cb = nullptr);

private:
    friend class AiHttpClosure;

    // Parse URL to extract host, port, and path
    static Status parse_url(const std::string& url, std::string& protocol, std::string& host, int& port,
                            std::string& path);
};

} // namespace starrocks