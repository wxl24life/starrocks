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

#include "exprs/ai/openai_compatible_provider.h"

#include <glog/logging.h>
#include <simdjson.h>

namespace starrocks {

std::string OpenAICompatibleProvider::build_chat_request(const std::string& model, const std::string& system_prompt,
                                                         const std::string& user_prompt,
                                                         const std::string& extra_params) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    w.StartObject();
    w.Key("model");
    w.String(model.c_str(), static_cast<rapidjson::SizeType>(model.size()));
    w.Key("messages");
    w.StartArray();
    // system message
    w.StartObject();
    w.Key("role");
    w.String("system");
    w.Key("content");
    w.String(system_prompt.c_str(), static_cast<rapidjson::SizeType>(system_prompt.size()));
    w.EndObject();
    // user message
    w.StartObject();
    w.Key("role");
    w.String("user");
    w.Key("content");
    w.String(user_prompt.c_str(), static_cast<rapidjson::SizeType>(user_prompt.size()));
    w.EndObject();
    w.EndArray();
    w.EndObject();
    return finalize_with_extra_params(buf, extra_params);
}

// ---------------------------------------------------------------------------
// Static extraction helpers
// ---------------------------------------------------------------------------

static std::string elem_to_string(simdjson::dom::element& elem) {
    return elem.is_string() ? std::string(elem.get_string().value()) : std::string(simdjson::minify(elem));
}

// Safely access arr[0][field1][field2]... without throwing on empty arrays.
static simdjson::error_code safe_first(simdjson::dom::element& arr, simdjson::dom::element& out) {
    if (!arr.is_array()) return simdjson::INCORRECT_TYPE;
    auto a = arr.get_array();
    if (a.size() == 0) return simdjson::INDEX_OUT_OF_BOUNDS;
    out = *a.begin();
    return simdjson::SUCCESS;
}

StatusOr<std::string> OpenAICompatibleProvider::extract_chat_content(simdjson::dom::element& doc) {
    // OpenAI error format: {"error": {"message": "..."}}
    simdjson::dom::element error_elem;
    if (doc["error"]["message"].get(error_elem) == simdjson::SUCCESS) {
        return Status::InternalError("AI Model Error: " + elem_to_string(error_elem));
    }

    // DashScope error format: {"code": "...", "message": "..."}
    simdjson::dom::element code_element;
    if (doc["code"].get(code_element) == simdjson::SUCCESS && code_element.is_string()) {
        std::string code = std::string(code_element.get_string().value());
        if (!code.empty()) {
            simdjson::dom::element msg_element;
            std::string msg = (doc["message"].get(msg_element) == simdjson::SUCCESS && msg_element.is_string())
                                      ? std::string(msg_element.get_string().value())
                                      : code;
            return Status::InternalError("AI Model Error: " + msg);
        }
    }

    simdjson::dom::element content, first;

    simdjson::dom::element choices;
    if (doc["choices"].get(choices) == simdjson::SUCCESS && safe_first(choices, first) == simdjson::SUCCESS &&
        first["message"]["content"].get(content) == simdjson::SUCCESS) {
        return elem_to_string(content);
    }

    if (doc["output"]["choices"].get(choices) == simdjson::SUCCESS && safe_first(choices, first) == simdjson::SUCCESS &&
        first["message"]["content"].get(content) == simdjson::SUCCESS) {
        return elem_to_string(content);
    }

    if (doc["output"]["text"].get(content) == simdjson::SUCCESS && content.is_string()) {
        return std::string(content.get_string().value());
    }

    return Status::InternalError("No valid AI response content");
}

// ---------------------------------------------------------------------------
// Token usage extraction from a parsed JSON document — pure, no side effects.
// ---------------------------------------------------------------------------

AITokenUsage OpenAICompatibleProvider::extract_token_usage(simdjson::dom::element& doc) {
    AITokenUsage usage;

    simdjson::dom::element usage_element;
    if (doc["usage"].get(usage_element) == simdjson::SUCCESS && usage_element.is_object()) {
        simdjson::dom::element t;
        if (usage_element["total_tokens"].get(t) == simdjson::SUCCESS && t.is_int64()) {
            usage.total_tokens = t.get_int64();
        }
        if (usage_element["prompt_tokens"].get(t) == simdjson::SUCCESS && t.is_int64()) {
            usage.prompt_tokens = t.get_int64();
        }
        if (usage_element["completion_tokens"].get(t) == simdjson::SUCCESS && t.is_int64()) {
            usage.completion_tokens = t.get_int64();
        }
        if (usage.prompt_tokens == 0 && usage_element["input_tokens"].get(t) == simdjson::SUCCESS && t.is_int64()) {
            usage.prompt_tokens = t.get_int64();
        }
        if (usage.completion_tokens == 0 && usage_element["output_tokens"].get(t) == simdjson::SUCCESS &&
            t.is_int64()) {
            usage.completion_tokens = t.get_int64();
        }
        simdjson::dom::element details;
        if (usage_element["prompt_tokens_details"].get(details) == simdjson::SUCCESS && details.is_object()) {
            if (details["cached_tokens"].get(t) == simdjson::SUCCESS && t.is_int64()) {
                usage.cached_tokens = t.get_int64();
            }
        }
        if (usage.total_tokens == 0 && (usage.prompt_tokens > 0 || usage.completion_tokens > 0)) {
            usage.total_tokens = usage.prompt_tokens + usage.completion_tokens;
        }
    }

    simdjson::dom::element model_elem;
    if (doc["model"].get(model_elem) == simdjson::SUCCESS && model_elem.is_string()) {
        usage.model = std::string(model_elem.get_string().value());
    } else if (doc["output"]["model"].get(model_elem) == simdjson::SUCCESS && model_elem.is_string()) {
        usage.model = std::string(model_elem.get_string().value());
    }

    return usage;
}

// ---------------------------------------------------------------------------
// parse_chat_response — single JSON parse,
// extract content + token usage into structured result.
// ---------------------------------------------------------------------------

StatusOr<AIChatResult> OpenAICompatibleProvider::parse_chat_response(const std::string& response) {
    if (response.empty()) {
        return Status::InternalError("AI response is empty");
    }
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element doc = parser.parse(response);
        auto content = extract_chat_content(doc);
        if (!content.ok()) return content.status();
        AIChatResult result;
        result.token_usage = extract_token_usage(doc);
        result.content = std::move(content.value());
        return result;
    } catch (const simdjson::simdjson_error& e) {
        return Status::InternalError(std::string("Invalid AI response format: ") + e.what());
    }
}

} // namespace starrocks
