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

#include "util/ai_http_client.h"

#include <brpc/channel.h>
#include <brpc/errno.pb.h>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/ai/ai_channel_pool.h"

namespace starrocks {

// Closure for async HTTP request.
// Holds a shared_ptr to the pooled channel so it stays alive during the async call.
class AiHttpClosure : public google::protobuf::Closure {
public:
    AiHttpClosure(std::shared_ptr<AiHttpContext> ctx, std::shared_ptr<brpc::Channel> channel)
            : _ctx(std::move(ctx)), _channel(std::move(channel)) {}

    ~AiHttpClosure() override = default;

    void Run() override {
        std::unique_ptr<AiHttpClosure> self_guard(this);

        if (_ctx->is_cancelled()) {
            VLOG(1) << "[AI] HttpClosure cancelled";
            _ctx->set_done(Status::Cancelled("AI HTTP request cancelled"), "");
            return;
        }

        int code = _ctx->_cntl.http_response().status_code();
        bool is_ehttp = _ctx->_cntl.Failed() && _ctx->_cntl.ErrorCode() == brpc::EHTTP;

        if (_ctx->_cntl.Failed() && !is_ehttp) {
            std::string error_msg = "AI HTTP request failed: " + std::string(berror(_ctx->_cntl.ErrorCode())) + ", " +
                                    _ctx->_cntl.ErrorText();
            _ctx->set_done(Status::InternalError(error_msg), "");
            return;
        }

        _ctx->_http_status_code = code;
        std::string body = _ctx->_cntl.response_attachment().to_string();
        _ctx->_cntl.response_attachment().clear();
        _ctx->_cntl.request_attachment().clear();
        if (code == 429) {
            _ctx->set_done(Status::ResourceBusy("AI rate limit exceeded (HTTP 429)"), std::move(body));
        } else if (code >= 400 && code < 500) {
            _ctx->set_done(
                    Status::InvalidArgument("AI client error (HTTP " + std::to_string(code) + ")"),
                    std::move(body));
        } else if (code >= 500) {
            _ctx->set_done(
                    Status::InternalError("AI server error (HTTP " + std::to_string(code) + ")"),
                    std::move(body));
        } else {
            _ctx->set_done(Status::OK(), std::move(body));
        }
    }

    brpc::Channel* channel() { return _channel.get(); }

private:
    std::shared_ptr<AiHttpContext> _ctx;
    std::shared_ptr<brpc::Channel> _channel;
};

void AiHttpContext::cancel() {
    if (_cancelled.exchange(true, std::memory_order_acq_rel)) {
        // Already cancelled
        return;
    }
    // Cancel the brpc call
    brpc::StartCancel(_call_id);
}

void AiHttpContext::set_done(const Status& status, std::string response) {
    _status = status;
    _response = std::move(response);
    _done.store(true, std::memory_order_release);
    if (_completion_cb) {
        _completion_cb();
    }
}


Status AiHttpClient::parse_url(const std::string& url, std::string& protocol, std::string& host, int& port,
                               std::string& path) {
    auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        return Status::InvalidArgument("Invalid URL format: " + url);
    }

    protocol = url.substr(0, scheme_end);
    if (protocol != "http" && protocol != "https") {
        return Status::InvalidArgument("Unsupported protocol: " + protocol);
    }

    size_t host_start = scheme_end + 3;
    size_t path_start = url.find('/', host_start);
    std::string host_port =
            (path_start == std::string::npos) ? url.substr(host_start) : url.substr(host_start, path_start - host_start);

    path = (path_start == std::string::npos) ? "/" : url.substr(path_start);

    auto colon_pos = host_port.find(':');
    if (colon_pos == std::string::npos) {
        host = host_port;
        port = (protocol == "https") ? 443 : 80;
    } else {
        host = host_port.substr(0, colon_pos);
        std::string port_str = host_port.substr(colon_pos + 1);
        if (port_str.empty() || port_str.size() > 5 ||
            port_str.find_first_not_of("0123456789") != std::string::npos) {
            return Status::InvalidArgument("Invalid port in URL: " + url);
        }
        port = std::stoi(port_str);
        if (port <= 0 || port > 65535) {
            return Status::InvalidArgument("Port out of range in URL: " + url);
        }
    }

    if (host.empty()) {
        return Status::InvalidArgument("Empty host in URL: " + url);
    }

    return Status::OK();
}

StatusOr<std::shared_ptr<AiHttpContext>> AiHttpClient::post_async(
        const std::string& url, const std::string& api_key, const std::string& post_data, int32_t timeout_ms,
        const std::vector<std::pair<std::string, std::string>>& auth_headers,
        std::function<void()> completion_cb) {
    // Parse URL
    std::string protocol, host, path;
    int port;
    auto parse_st = parse_url(url, protocol, host, port, path);
    if (!parse_st.ok()) {
        return parse_st;
    }

    if (timeout_ms <= 0) {
        timeout_ms = config::ai_function_http_timeout_ms;
    }

    // Get or create a pooled channel for this endpoint
    auto channel_result = AIChannelPool::instance()->get_or_create(host, port, protocol);
    if (!channel_result.ok()) {
        return channel_result.status();
    }
    auto channel = std::move(channel_result.value());

    // Create context — set per-request timeouts on the controller
    auto ctx = std::make_shared<AiHttpContext>();
    ctx->_cntl.set_timeout_ms(timeout_ms);

    ctx->_cntl.http_request().uri() = path;
    ctx->_cntl.http_request().SetHeader("Host", host);
    ctx->_cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    ctx->_cntl.http_request().set_content_type("application/json");
    if (auth_headers.empty()) {
        ctx->_cntl.http_request().SetHeader("Authorization", "Bearer " + api_key);
    } else {
        for (const auto& [key, value] : auth_headers) {
            ctx->_cntl.http_request().SetHeader(key, value);
        }
    }
    ctx->_cntl.request_attachment().append(post_data);

    ctx->_call_id = ctx->_cntl.call_id();

    // Set completion callback BEFORE CallMethod to avoid the race where brpc
    // completes the request before the caller can register the callback.
    // This mirrors StarRocks' BThreadCountDownLatch pattern: the notification
    // mechanism is always set up before the async operation begins.
    if (completion_cb) {
        ctx->set_completion_callback(std::move(completion_cb));
    }

    auto* closure = new AiHttpClosure(ctx, channel);
    closure->channel()->CallMethod(nullptr, &ctx->_cntl, nullptr, nullptr, closure);

    return ctx;
}

} // namespace starrocks
