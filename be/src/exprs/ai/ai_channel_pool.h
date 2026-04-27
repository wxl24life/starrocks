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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/statusor.h"

namespace brpc {
class Channel;
}

namespace starrocks {

// Per-endpoint brpc::Channel pool with idle eviction.
// A single brpc::Channel internally pools TCP connections and supports
// concurrent RPCs, so we only need one Channel per (host:port, protocol) tuple.
// Timeout is set per-Controller rather than per-Channel to allow sharing.
//
// Channels idle for more than kIdleEvictionUs (10 minutes) are lazily evicted
// during get_or_create() to prevent unbounded accumulation when endpoints
// change (e.g. different AI models/providers across queries).
class AIChannelPool {
public:
    static AIChannelPool* instance();

    // Get or create a channel for the given endpoint.
    // The returned shared_ptr keeps the channel alive for the duration of the request.
    StatusOr<std::shared_ptr<brpc::Channel>> get_or_create(const std::string& host, int port,
                                                            const std::string& protocol);

    void shutdown();

    // Visible for testing.
    size_t size() const;

private:
    AIChannelPool() = default;
    ~AIChannelPool() = default;

    static constexpr int64_t kIdleEvictionUs = 10LL * 60 * 1'000'000;  // 10 minutes
    static constexpr int64_t kEvictionCheckIntervalUs = 60LL * 1'000'000;  // 1 minute

    struct ChannelEntry {
        std::shared_ptr<brpc::Channel> channel;
        int64_t last_access_us = 0;
    };

    void _evict_idle_locked(int64_t now_us);

    mutable bthread::Mutex _mutex;
    std::unordered_map<std::string, ChannelEntry> _channels;
    int64_t _last_eviction_us = 0;
};

} // namespace starrocks
