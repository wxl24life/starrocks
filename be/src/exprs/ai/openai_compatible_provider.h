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

#include "exprs/ai/ai_provider.h"

namespace simdjson {
namespace dom {
class element;
} // namespace dom
} // namespace simdjson

namespace starrocks {

// Provider for OpenAI-compatible APIs (OpenAI, DeepSeek, DashScope compatible-mode, vLLM, Ollama, etc.).
class OpenAICompatibleProvider final : public AIProvider {
public:
    OpenAICompatibleProvider() = default;
    ~OpenAICompatibleProvider() override = default;

    std::string build_chat_request(const std::string& model, const std::string& system_prompt,
                                   const std::string& user_prompt, const std::string& extra_params = "") override;

    StatusOr<AIChatResult> parse_chat_response(const std::string& response) override;

    // Static extraction helper for content from a parsed JSON doc.
    static StatusOr<std::string> extract_chat_content(simdjson::dom::element& doc);

    // Extract token usage metrics and model name from a parsed JSON doc.
    static AITokenUsage extract_token_usage(simdjson::dom::element& doc);
};

} // namespace starrocks
