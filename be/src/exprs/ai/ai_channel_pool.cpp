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

#include "exprs/ai/ai_channel_pool.h"

#include <brpc/channel.h>
#include <butil/time.h>

#include "common/config.h"
#include "common/logging.h"

namespace starrocks {

// Channel-level timeout upper bound — generous safety net.
// Per-request timeout is set via Controller::set_timeout_ms() in post_async().
static constexpr int kChannelTimeoutMs = 120'000;

AIChannelPool* AIChannelPool::instance() {
    static AIChannelPool s_instance;
    return &s_instance;
}

StatusOr<std::shared_ptr<brpc::Channel>> AIChannelPool::get_or_create(const std::string& host, int port,
                                                                       const std::string& protocol) {
    std::string key = protocol + "://" + host + ":" + std::to_string(port);
    const int64_t now_us = butil::gettimeofday_us();

    {
        std::lock_guard<bthread::Mutex> lk(_mutex);

        // Lazy eviction of idle channels (at most once per minute).
        if (now_us - _last_eviction_us >= kEvictionCheckIntervalUs) {
            _evict_idle_locked(now_us);
        }

        auto it = _channels.find(key);
        if (it != _channels.end()) {
            it->second.last_access_us = now_us;
            return it->second.channel;
        }
    }

    // Channel not found — create outside the lock, then insert under lock.
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    // Channel-level timeout is a generous upper bound; the per-request
    // timeout is set via Controller::set_timeout_ms() in post_async().
    // connect_timeout_ms cannot be overridden per-request in brpc, so
    // it is read from config here. Changes take effect when a new Channel
    // is created (after idle eviction or BE restart), matching the pattern
    // used by PInternalService_RecoverableStub::reset_channel().
    options.timeout_ms = kChannelTimeoutMs;
    options.connect_timeout_ms = config::ai_function_http_connect_timeout_ms;
    options.max_retry = 0;

    if (protocol == "https") {
        auto* ssl_options = options.mutable_ssl_options();
        ssl_options->sni_name = host;
    }

    auto channel = std::make_shared<brpc::Channel>();
    std::string server_addr = host + ":" + std::to_string(port);

    if (channel->Init(server_addr.c_str(), &options) != 0) {
        return Status::InternalError("Failed to init brpc channel to " + server_addr);
    }

    {
        std::lock_guard<bthread::Mutex> lk(_mutex);
        // Double-check: another thread may have inserted concurrently.
        auto [it, inserted] = _channels.emplace(key, ChannelEntry{channel, now_us});
        if (!inserted) {
            it->second.last_access_us = now_us;
        }
        return it->second.channel;
    }
}

void AIChannelPool::_evict_idle_locked(int64_t now_us) {
    _last_eviction_us = now_us;
    for (auto it = _channels.begin(); it != _channels.end();) {
        // Only evict channels not held by any in-flight request (use_count == 1
        // means only the pool holds a reference).
        if (now_us - it->second.last_access_us > kIdleEvictionUs && it->second.channel.use_count() == 1) {
            it = _channels.erase(it);
        } else {
            ++it;
        }
    }
}

void AIChannelPool::shutdown() {
    std::lock_guard<bthread::Mutex> lk(_mutex);
    _channels.clear();
}

size_t AIChannelPool::size() const {
    std::lock_guard<bthread::Mutex> lk(_mutex);
    return _channels.size();
}

} // namespace starrocks
