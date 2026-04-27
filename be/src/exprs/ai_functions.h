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

#include <functional>
#include <optional>

#include "column/array_column.h"
#include "column/column_viewer.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

class AIProvider;
struct AITask;

class AiFunctions {
public:
    DEFINE_VECTORIZED_FN(ai_sentiment);
    DEFINE_VECTORIZED_FN(ai_classify);
    DEFINE_VECTORIZED_FN(ai_extract);
    DEFINE_VECTORIZED_FN(ai_fix_grammar);
    DEFINE_VECTORIZED_FN(ai_redact);
    DEFINE_VECTORIZED_FN(ai_translate);
    DEFINE_VECTORIZED_FN(ai_similarity);
    DEFINE_VECTORIZED_FN(ai_summarize);
    DEFINE_VECTORIZED_FN(ai_complete);
    DEFINE_VECTORIZED_FN(ai_custom_query);
    DEFINE_VECTORIZED_FN(ai_filter);

    // Per-row task builder: returns post_data for the row, or nullopt to mark as NULL.
    using RowTaskBuilder = std::function<std::optional<std::string>(size_t row)>;

    // Converts dispatched string results into a typed column.
    using ChatColumnBuilder = ColumnPtr (*)(const std::vector<std::string>&, const std::vector<uint8_t>&, size_t);

    // Unified entry point for all chat-type AI functions.
    // Dispatches LLM requests and converts results via column_builder (VARCHAR or JSON).
    static StatusOr<ColumnPtr> execute_chat_function(FunctionContext* context, size_t num_rows,
                                                     const RowTaskBuilder& build_post_data,
                                                     ChatColumnBuilder column_builder,
                                                     const std::string& endpoint = "",
                                                     const std::string& api_key = "",
                                                     AIProvider* provider = nullptr);

    // Column builders — each converts (results, is_null, size) into a specific column type.
    static ColumnPtr build_varchar_column(const std::vector<std::string>& results, const std::vector<uint8_t>& is_null,
                                          size_t size);
    static ColumnPtr build_json_column(const std::vector<std::string>& results, const std::vector<uint8_t>& is_null,
                                       size_t size);

    static StatusOr<std::string> encode_array_arg_to_json(const ColumnPtr& column, size_t row);

private:
    // Build AITasks from RowTaskBuilder, dispatch them, and collect parsed string results.
    // Shared by execute_chat_function and custom-column functions (ai_filter, ai_similarity).
    static Status dispatch_chat_tasks(FunctionContext* context, size_t num_rows,
                                      const RowTaskBuilder& build_post_data,
                                      std::vector<std::string>& results, std::vector<uint8_t>& is_null,
                                      const std::string& endpoint = "", const std::string& api_key = "",
                                      AIProvider* provider = nullptr);

    // Blocking: dispatch tasks via AITaskDispatcher, collect responses, and parse via provider.
    static Status dispatch_and_collect(std::vector<AITask>& tasks, std::vector<std::string>& results,
                                       FunctionContext* context,
                                       std::vector<uint8_t>& null_flags, AIProvider* provider = nullptr);

    static std::string encode_array_to_json_string(const ArrayColumn* array_column, size_t row);
};

} // namespace starrocks
