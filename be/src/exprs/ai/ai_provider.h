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

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/statusor.h"

namespace starrocks {

// Token usage metrics extracted from an AI API response.
struct AITokenUsage {
    int64_t total_tokens = 0;
    int64_t prompt_tokens = 0;
    int64_t completion_tokens = 0;
    int64_t cached_tokens = 0;
    std::string model;

    AITokenUsage& operator+=(const AITokenUsage& rhs) {
        total_tokens += rhs.total_tokens;
        prompt_tokens += rhs.prompt_tokens;
        completion_tokens += rhs.completion_tokens;
        cached_tokens += rhs.cached_tokens;
        if (model.empty()) model = rhs.model;
        return *this;
    }
};

// Structured result from parsing a chat completion response.
struct AIChatResult {
    std::string content;
    AITokenUsage token_usage;
};

// Escape a string for embedding inside a JSON string value per RFC 8259 Section 7.
// Delegates to rapidjson::Writer which handles all edge cases including surrogate
// pairs, BOM, control characters, etc.  The result is the *inner* content of a
// JSON string literal (without surrounding quotes).
inline std::string json_escape_string(const std::string& s) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    writer.String(s.c_str(), static_cast<rapidjson::SizeType>(s.size()));
    const char* p = buf.GetString();
    size_t len = buf.GetSize();
    if (len >= 2 && p[0] == '"' && p[len - 1] == '"') {
        return {p + 1, len - 2};
    }
    return {p, len};
}

// Merge extra_params JSON object into the request JSON serialized in |buf|.
// extra_params must be a valid JSON object string like '{"temperature": 0.7}'.
// Duplicate keys in extra_params overwrite the base value.
inline std::string finalize_with_extra_params(rapidjson::StringBuffer& buf, const std::string& extra_params) {
    std::string base(buf.GetString(), buf.GetSize());
    if (extra_params.empty()) {
        return base;
    }

    rapidjson::Document doc;
    if (doc.Parse(base.c_str()).HasParseError() || !doc.IsObject()) {
        LOG(WARNING) << "[AI] finalize_with_extra_params: base JSON parse failed";
        return base;
    }

    rapidjson::Document extra_doc;
    if (extra_doc.Parse(extra_params.c_str()).HasParseError() || !extra_doc.IsObject()) {
        LOG(WARNING) << "[AI] finalize_with_extra_params: extra_params parse failed: "
                     << extra_params.substr(0, 256);
        return base;
    }

    for (auto it = extra_doc.MemberBegin(); it != extra_doc.MemberEnd(); ++it) {
        rapidjson::Value key(it->name, doc.GetAllocator());
        rapidjson::Value val(it->value, doc.GetAllocator());
        if (doc.HasMember(it->name)) {
            doc[it->name] = val;
        } else {
            doc.AddMember(key, val, doc.GetAllocator());
        }
    }

    rapidjson::StringBuffer out;
    rapidjson::Writer<rapidjson::StringBuffer> writer(out);
    doc.Accept(writer);
    return {out.GetString(), out.GetSize()};
}

// Abstract interface for AI model providers.
// Implementations handle request building and response parsing for different API formats.
// Provider is a pure parser — it does NOT perform side effects like token accumulation.
class AIProvider {
public:
    virtual ~AIProvider() = default;

    virtual std::string build_chat_request(const std::string& model, const std::string& system_prompt,
                                           const std::string& user_prompt,
                                           const std::string& extra_params = "") = 0;

    // Parse chat completion response: extract content and token usage.
    virtual StatusOr<AIChatResult> parse_chat_response(const std::string& response) = 0;

    // Return provider-specific auth headers. Empty vector means use AiHttpClient's
    // default "Authorization: Bearer <api_key>" behavior.
    virtual std::vector<std::pair<std::string, std::string>> get_auth_headers(const std::string& api_key) const {
        return {};
    }
};

} // namespace starrocks
