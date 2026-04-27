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

#include "exprs/ai_functions.h"

#include <simdjson.h>

#include <cerrno>
#include <cmath>
#include <optional>
#include <vector>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "types/logical_type.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "exprs/ai/ai_default_configuration.h"
#include "exprs/ai/ai_provider.h"
#include "exprs/ai/ai_provider_registry.h"
#include "exprs/ai/ai_result_cache.h"
#include "exprs/ai/ai_task_dispatcher.h"
#include "fmt/format.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/ai_http_client.h"
#include "util/uid_util.h"

namespace starrocks {

using pipeline::QueryContext;

namespace {

void fill_ai_task_ids(AITask& task, FunctionContext* context) {
    if (context == nullptr || context->state() == nullptr) {
        return;
    }
    auto* st = context->state();
    task.query_id = print_id(st->query_id());
    task.fragment_instance_id = print_id(st->fragment_instance_id());
}

std::string query_id_for_log(FunctionContext* context, const std::vector<AITask>& tasks) {
    if (!tasks.empty() && !tasks[0].query_id.empty()) {
        return tasks[0].query_id;
    }
    if (context != nullptr && context->state() != nullptr) {
        return print_id(context->state()->query_id());
    }
    return "n/a";
}

} // namespace

// ---------------------------------------------------------------------------
// Hardcoded prompts — black-box design aligned with Snowflake/Databricks.
// Users cannot modify these; upgrades automatically improve prompt quality.
// ---------------------------------------------------------------------------

static constexpr const char* kSystemPromptDefault = "You are a helpful assistant.";

static constexpr const char* kSystemPromptClassifier =
        "You are a precise text analysis assistant. Follow the instructions exactly. "
        "Output only what is requested, nothing else.";

static constexpr const char* kSystemPromptFilter =
        "You are a boolean classifier. You MUST respond with exactly 'true' or 'false'. "
        "Do not include any other text, explanation, or punctuation.";

static constexpr const char* kPromptSentiment =
        "Analyze the overall sentiment of the following text. "
        "Output exactly one lowercase word from this list: positive, negative, neutral, mixed, unknown. "
        "No punctuation, no explanation.\n\nText: $0";

static constexpr const char* kPromptClassify =
        "Classify the following text into exactly one of these categories: $0.\n"
        "Return a JSON object in this exact format: {\"labels\": [\"<chosen_category>\"]}\n"
        "The array must contain exactly one string that matches one of the given categories.\n"
        "Output only valid JSON, no markdown, no explanation.\n\nText: $1";

static constexpr const char* kPromptExtract =
        "Extract a value for each of the following keys from the text below.\n"
        "Keys: $0\n"
        "For each key, extract exactly one value. If a key's value is not found, use null.\n"
        "Return a JSON object in this exact format: {\"response\": {\"key1\": \"value1\", \"key2\": null}}\n"
        "Output only valid JSON, no markdown, no explanation.\n\nText: $1";

static constexpr const char* kPromptFixGrammar =
        "Fix the grammar and spelling of the following text. "
        "Preserve the original meaning and tone. Output only the corrected text, nothing else.\n\nText: $0";

static constexpr const char* kPromptRedact =
        "Redact personally identifiable information (PII) in the text below.\n"
        "Categories to redact: $0\n"
        "Replace each detected PII value with its uppercase category name in square brackets, "
        "e.g. [NAME], [ADDRESS], [EMAIL], [PHONE], [SSN].\n"
        "If no PII is found, return the original text unchanged. "
        "Output only the redacted text, nothing else.\n\nText: $1";

static constexpr const char* kPromptTranslate =
        "Translate the following text from $0 into $1. "
        "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: $2";

static constexpr const char* kPromptTranslateAutoDetect =
        "Translate the following text into $0. Auto-detect the source language. "
        "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: $1";

static constexpr const char* kPromptSimilarity =
        "Calculate the semantic similarity between the following two texts.\n"
        "Output only a single decimal number between 0.00 and 1.00 (0 = completely different, "
        "1 = identical meaning). No explanation, no extra text.\n\n"
        "Text 1: $0\nText 2: $1";

static constexpr const char* kPromptSummarize =
        "Summarize the following text concisely, capturing the key points. "
        "Output only the summary, nothing else.\n\nText: $0";

static constexpr const char* kPromptFilter =
        "Given the following text, determine if this condition is true. "
        "You MUST respond with exactly 'true' or 'false' and nothing else.\n"
        "Text: $0\nCondition: $1";

// Extra params for structured JSON output — auto-injected for JSON-returning functions.
static constexpr const char* kExtraParamsJsonFormat = R"({"response_format": {"type": "json_object"}})";

static constexpr size_t kErrorResponseTruncateLen = 256;

static std::string extract_api_error_detail(const std::string& response, const Status& status) {
    if (!response.empty()) {
        try {
            simdjson::dom::parser parser;
            simdjson::dom::element doc = parser.parse(response);
            simdjson::dom::element msg;
            if (doc["error"]["message"].get(msg) == simdjson::SUCCESS && msg.is_string()) {
                return std::string(msg.get_string().value());
            }
            simdjson::dom::element code_elem;
            if (doc["code"].get(code_elem) == simdjson::SUCCESS && code_elem.is_string()) {
                std::string code = std::string(code_elem.get_string().value());
                if (!code.empty()) {
                    simdjson::dom::element msg_elem;
                    if (doc["message"].get(msg_elem) == simdjson::SUCCESS && msg_elem.is_string()) {
                        return std::string(msg_elem.get_string().value());
                    }
                    return code;
                }
            }
        } catch (...) {
        }
        return response.substr(0, kErrorResponseTruncateLen);
    }
    return status.to_string();
}

// Safe replacement for RETURN_IF_COLUMNS_ONLY_NULL that always returns the
// first column (which matches the return type for AI functions where columns[0]
// is the text input). RETURN_IF_COLUMNS_ONLY_NULL returns whichever column is
// only_null() first, which may have the wrong type for multi-arg functions.
#define RETURN_IF_ANY_AI_COLUMN_NULL(columns)                       \
    do {                                                            \
        for (const auto& col : columns) {                           \
            if (col->only_null()) {                                 \
                return ColumnHelper::create_const_null_column(       \
                        columns[0]->size());                        \
            }                                                       \
        }                                                           \
    } while (0)

// Retrieve the AIModelConfiguration injected by AIScanOperatorFactory via
// FunctionContext::set_function_state(FRAGMENT_LOCAL).
// Returns a reference — never null; falls back to a static empty default.
static const AIModelConfiguration& get_ai_config(FunctionContext* context) {
    static const AIModelConfiguration kEmpty;
    auto* cfg = static_cast<const AIModelConfiguration*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return cfg ? *cfg : kEmpty;
}

// ---------------------------------------------------------------------------
// Helpers — null-safe access to QueryContext + token usage accumulation.
// ---------------------------------------------------------------------------

static QueryContext* get_query_ctx(FunctionContext* context) {
    if (context == nullptr) return nullptr;
    auto* state = context->state();
    if (state == nullptr) return nullptr;
    return state->query_ctx();
}

static void accumulate_token_usage(FunctionContext* context, const AITokenUsage& usage) {
    auto* query_ctx = get_query_ctx(context);
    if (query_ctx == nullptr) return;
    query_ctx->add_ai_function_total_token_usage(usage.total_tokens);
    query_ctx->add_ai_function_prompt_token_usage(usage.prompt_tokens);
    query_ctx->add_ai_function_completion_token_usage(usage.completion_tokens);
    query_ctx->add_ai_function_cached_token_usage(usage.cached_tokens);
    query_ctx->add_ai_token_usage_by_key(context->function_name(), usage.model, usage.total_tokens,
                                         usage.prompt_tokens, usage.completion_tokens, usage.cached_tokens);
}

// ---------------------------------------------------------------------------
// Layer 1: dispatch tasks -> collect responses -> parse via provider
// ---------------------------------------------------------------------------

Status AiFunctions::dispatch_and_collect(std::vector<AITask>& tasks, std::vector<std::string>& results,
                                         FunctionContext* context,
                                         std::vector<uint8_t>& null_flags, AIProvider* provider) {
    if (provider == nullptr) provider = AIProviderRegistry::instance()->default_provider();
    auto* cache = AIResultCache::instance();

    // Report stats to QueryContext for query-level profile counters.
    auto report_stats = [context](int64_t cache_hits, int64_t http_calls) {
        auto* query_ctx = get_query_ctx(context);
        if (query_ctx == nullptr) return;
        if (cache_hits > 0) query_ctx->add_ai_cache_hits(cache_hits);
        if (http_calls > 0) query_ctx->add_ai_http_calls(http_calls);
    };

    // Phase 1: check cache, partition into hits and misses
    std::vector<AITask> uncached_tasks;
    uncached_tasks.reserve(tasks.size());
    for (auto& task : tasks) {
        std::string cached;
        if (cache->lookup(task.post_data, &cached)) {
            results[task.row_index] = std::move(cached);
            continue;
        }
        uncached_tasks.push_back(std::move(task));
    }

    int64_t cache_hits = static_cast<int64_t>(tasks.size() - uncached_tasks.size());
    const std::string qid = query_id_for_log(context, uncached_tasks.empty() ? tasks : uncached_tasks);
    if (uncached_tasks.empty()) {
        LOG(INFO) << "[AI] phase=dispatch_and_collect_cached query_id=" << qid << " hits=" << cache_hits;
        report_stats(cache_hits, 0);
        return Status::OK();
    }

    // Phase 2: dispatch only cache misses
    RETURN_IF_ERROR(AITaskDispatcher::dispatch(uncached_tasks));
    const bool fail_on_error = (config::ai_function_on_error.value() == "fail");

    int error_count = 0;
    int ok_count = 0;
    std::string first_error;
    AITokenUsage batch_usage;
    for (auto& task : uncached_tasks) {
        if (!task.http_ctx) {
            if (fail_on_error) {
                return Status::InternalError("Failed to send HTTP request for row " +
                                             std::to_string(task.row_index));
            }
            ++error_count;
            null_flags[task.row_index] = 1;
            continue;
        }

        int http_code = task.http_ctx->http_status_code();
        if (!task.http_ctx->status().ok() || http_code < 200 || http_code >= 300) {
            std::string err = fmt::format(
                    "AI HTTP {} for row {}: {}", http_code, task.row_index,
                    extract_api_error_detail(task.http_ctx->response(), task.http_ctx->status()));
            task.http_ctx->release_resources();
            task.http_ctx.reset();
            if (fail_on_error) {
                return Status::InternalError(err);
            }
            if (first_error.empty()) first_error = std::move(err);
            ++error_count;
            null_flags[task.row_index] = 1;
            continue;
        }

        auto chat_result = provider->parse_chat_response(task.http_ctx->response());
        task.http_ctx->release_resources();
        task.http_ctx.reset();
        if (!chat_result.ok()) {
            if (fail_on_error) {
                return chat_result.status();
            }
            ++error_count;
            null_flags[task.row_index] = 1;
        } else {
            accumulate_token_usage(context, chat_result->token_usage);
            batch_usage += chat_result->token_usage;
            ++ok_count;
            results[task.row_index] = std::move(chat_result->content);
            cache->insert(task.post_data, results[task.row_index]);
        }
        task.post_data.clear();
    }
    if (error_count > 0) {
        LOG(WARNING) << "[AI] phase=dispatch_and_collect_done query_id=" << qid << " errors=" << error_count << "/"
                     << uncached_tasks.size() << " first_error=" << first_error;

        // Report error stats to QueryContext for audit logging.
        if (auto* query_ctx = get_query_ctx(context); query_ctx != nullptr) {
            query_ctx->add_ai_error_rows(error_count, first_error);
        }

        // All requests failed → likely a configuration error (wrong model, invalid API key,
        // or unreachable endpoint). Surface the error instead of returning all NULLs silently.
        if (!fail_on_error && error_count == static_cast<int>(uncached_tasks.size())) {
            return Status::InternalError(fmt::format(
                    "All {} AI requests failed: {}", error_count, first_error));
        }
    }

    const std::string func_name = context ? context->function_name() : "unknown";
    LOG(INFO) << "[AI] phase=dispatch_and_collect_done query_id=" << qid
              << " func=" << func_name
              << " ok=" << ok_count << " errors=" << error_count
              << " cache_hits=" << cache_hits
              << " model=" << batch_usage.model
              << " tokens(total=" << batch_usage.total_tokens
              << " prompt=" << batch_usage.prompt_tokens
              << " completion=" << batch_usage.completion_tokens
              << " cached=" << batch_usage.cached_tokens << ")";
    report_stats(cache_hits, static_cast<int64_t>(uncached_tasks.size()));
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Layer 2a: build tasks from RowTaskBuilder, dispatch, collect string results
// ---------------------------------------------------------------------------

Status AiFunctions::dispatch_chat_tasks(FunctionContext* context, size_t num_rows,
                                        const RowTaskBuilder& build_post_data,
                                        std::vector<std::string>& results, std::vector<uint8_t>& is_null,
                                        const std::string& endpoint, const std::string& api_key,
                                        AIProvider* provider) {
    if (provider == nullptr) provider = AIProviderRegistry::instance()->default_provider();
    auto auth_headers = api_key.empty() ? std::vector<std::pair<std::string, std::string>>{}
                                        : provider->get_auth_headers(api_key);

    const std::atomic<bool>* cancel_flag =
            context->state() != nullptr ? &context->state()->cancelled_ref() : nullptr;

    std::vector<AITask> tasks;
    for (size_t row = 0; row < num_rows; ++row) {
        auto post_data = build_post_data(row);
        if (!post_data.has_value()) {
            is_null[row] = 1;
            continue;
        }
        AITask task;
        task.row_index = row;
        task.post_data = std::move(post_data.value());
        task.endpoint = endpoint;
        task.api_key = api_key;
        task.auth_headers = auth_headers;
        task.cancelled = cancel_flag;
        fill_ai_task_ids(task, context);
        tasks.push_back(std::move(task));
    }
    return dispatch_and_collect(tasks, results, context, is_null, provider);
}

// ---------------------------------------------------------------------------
// Layer 2b: unified chat function entry point (dispatch + column build)
// ---------------------------------------------------------------------------

StatusOr<ColumnPtr> AiFunctions::execute_chat_function(FunctionContext* context, size_t num_rows,
                                                       const RowTaskBuilder& build_post_data,
                                                       ChatColumnBuilder column_builder,
                                                       const std::string& endpoint, const std::string& api_key,
                                                       AIProvider* provider) {
    std::vector<std::string> results(num_rows);
    std::vector<uint8_t> is_null(num_rows, 0);
    RETURN_IF_ERROR(dispatch_chat_tasks(context, num_rows, build_post_data, results, is_null, endpoint,
                                        api_key, provider));
    return column_builder(results, is_null, num_rows);
}

// ---------------------------------------------------------------------------
// Map parameter helpers (for MAP parameter parsing in ai_complete)
// ---------------------------------------------------------------------------

// Build extra_params JSON directly from MapColumn's underlying keys/values columns.
static std::string map_column_to_extra_params(const MapColumn& map_col, size_t idx) {
    auto [offset, sz] = map_col.get_map_offset_size(idx);
    if (sz == 0) return "";

    // Read keys and values directly from the underlying columns, avoiding
    // ColumnViewer which may mis-handle nullable BinaryColumn in some cases.
    const auto& keys_col = map_col.keys_column();
    const auto& vals_col = map_col.values_column();

    // Helper: extract string from a column element (handles nullable/binary)
    auto get_string = [](const ColumnPtr& col, size_t row, bool& is_null) -> std::string {
        if (col->is_nullable()) {
            auto* nc = down_cast<const NullableColumn*>(col.get());
            if (nc->is_null(row)) {
                is_null = true;
                return "";
            }
            auto* bc = down_cast<const BinaryColumn*>(nc->data_column().get());
            is_null = false;
            return bc->get_slice(row).to_string();
        } else {
            auto* bc = down_cast<const BinaryColumn*>(col.get());
            is_null = false;
            return bc->get_slice(row).to_string();
        }
    };

    std::string result = "{";
    bool first = true;
    for (size_t i = 0; i < sz; ++i) {
        bool key_null = false;
        std::string key = get_string(keys_col, offset + i, key_null);
        if (key_null || key.empty()) continue;

        if (!first) result += ", ";
        first = false;
        result += "\"" + json_escape_string(key) + "\": ";

        bool val_null = false;
        std::string val = get_string(vals_col, offset + i, val_null);
        if (val_null) {
            result += "null";
        } else if (val == "true" || val == "false" || val == "null") {
            result += val;
        } else {
            char* end = nullptr;
            errno = 0;
            double num = std::strtod(val.c_str(), &end);
            if (errno == 0 && end == val.c_str() + val.size() && std::isfinite(num)) {
                result += val;
            } else if (val.size() >= 2 &&
                       ((val.front() == '[' && val.back() == ']') ||
                        (val.front() == '{' && val.back() == '}'))) {
                // JSON array/object pass-through (e.g. stop=["token"])
                static thread_local simdjson::dom::parser tl_parser;
                if (tl_parser.parse(val).error() == simdjson::SUCCESS) {
                    result += val;
                } else {
                    result += "\"" + json_escape_string(val) + "\"";
                }
            } else {
                result += "\"" + json_escape_string(val) + "\"";
            }
        }
    }
    return result + "}";
}

// ---------------------------------------------------------------------------
// Data-driven AI function descriptor for the common pattern:
//   text(columns[0]) + optional array_arg(columns[1]) → prompt → chat → column
// ---------------------------------------------------------------------------
struct AIChatFuncDesc {
    const char* system_prompt;
    const char* prompt_template;  // strings::Substitute template ($0=array_arg or text, $1=text)
    const char* extra_params;     // nullptr or kExtraParamsJsonFormat
    AiFunctions::ChatColumnBuilder column_builder;
    bool has_array_arg;           // true if columns[1] is ARRAY (e.g. labels, categories)
};

// Shared implementation for simple 1-text + optional-array AI functions.
static StatusOr<ColumnPtr> execute_descriptored_ai(FunctionContext* context, const Columns& columns,
                                                    const AIChatFuncDesc& desc) {
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->default_provider();
    std::string extra = desc.extra_params ? desc.extra_params : "";

    if (desc.has_array_arg) {
        DCHECK_GE(columns.size(), 2);
        DCHECK(columns[1]->is_constant()) << "array arg column must be const-folded by FE";
        ASSIGN_OR_RETURN(std::string array_arg, AiFunctions::encode_array_arg_to_json(columns[1], 0));
        return AiFunctions::execute_chat_function(
                context, columns[0]->size(),
                [&](size_t row) -> std::optional<std::string> {
                    if (viewer.is_null(row)) return std::nullopt;
                    return provider->build_chat_request(
                            cfg.model, desc.system_prompt,
                            strings::Substitute(desc.prompt_template, array_arg, viewer.value(row).to_string()),
                            extra);
                },
                desc.column_builder, cfg.endpoint, cfg.api_key);
    }

    // Single-arg: $0 = text
    return AiFunctions::execute_chat_function(
            context, columns[0]->size(),
            [&](size_t row) -> std::optional<std::string> {
                if (viewer.is_null(row)) return std::nullopt;
                return provider->build_chat_request(
                        cfg.model, desc.system_prompt,
                        strings::Substitute(desc.prompt_template, viewer.value(row).to_string()), extra);
            },
            desc.column_builder, cfg.endpoint, cfg.api_key);
}

// ---------------------------------------------------------------------------
// AI function implementations
// ---------------------------------------------------------------------------

StatusOr<ColumnPtr> AiFunctions::ai_sentiment(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    static const AIChatFuncDesc kDesc{kSystemPromptClassifier, kPromptSentiment, nullptr, build_varchar_column, false};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_classify(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    static const AIChatFuncDesc kDesc{kSystemPromptClassifier, kPromptClassify, kExtraParamsJsonFormat,
                                      build_json_column, true};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_extract(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    static const AIChatFuncDesc kDesc{kSystemPromptClassifier, kPromptExtract, kExtraParamsJsonFormat,
                                      build_json_column, true};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_fix_grammar(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    static const AIChatFuncDesc kDesc{kSystemPromptDefault, kPromptFixGrammar, nullptr, build_varchar_column, false};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_redact(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    static const AIChatFuncDesc kDesc{kSystemPromptClassifier, kPromptRedact, nullptr, build_varchar_column, true};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_translate(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 3);
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto text_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto src_lang_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto tgt_lang_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->default_provider();
    return execute_chat_function(
            context, columns[0]->size(),
            [&](size_t row) -> std::optional<std::string> {
                if (text_viewer.is_null(row) || tgt_lang_viewer.is_null(row)) return std::nullopt;
                std::string tgt_lang = tgt_lang_viewer.value(row).to_string();
                if (tgt_lang.empty()) return std::nullopt;
                std::string src_lang =
                        src_lang_viewer.is_null(row) ? "" : src_lang_viewer.value(row).to_string();
                if (src_lang.empty()) {
                    return provider->build_chat_request(
                            cfg.model, kSystemPromptDefault,
                            strings::Substitute(kPromptTranslateAutoDetect, tgt_lang,
                                                text_viewer.value(row).to_string()));
                }
                return provider->build_chat_request(
                        cfg.model, kSystemPromptDefault,
                        strings::Substitute(kPromptTranslate, src_lang, tgt_lang,
                                            text_viewer.value(row).to_string()));
            },
            build_varchar_column, cfg.endpoint, cfg.api_key);
}

StatusOr<ColumnPtr> AiFunctions::ai_similarity(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto v1 = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto v2 = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto size = columns[0]->size();
    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->default_provider();

    std::vector<std::string> str_results(size);
    std::vector<uint8_t> is_null(size, 0);

    RETURN_IF_ERROR(dispatch_chat_tasks(
            context, size,
            [&](size_t row) -> std::optional<std::string> {
                if (v1.is_null(row) || v2.is_null(row)) return std::nullopt;
                return provider->build_chat_request(
                        cfg.model, kSystemPromptDefault,
                        strings::Substitute(kPromptSimilarity, v1.value(row).to_string(),
                                            v2.value(row).to_string()));
            },
            str_results, is_null, cfg.endpoint, cfg.api_key));

    auto data_column = FloatColumn::create();
    data_column->reserve(size);
    NullColumnPtr null_column = NullColumn::create();
    null_column->reserve(size);
    for (size_t i = 0; i < size; ++i) {
        null_column->append(is_null[i]);
        if (!is_null[i]) {
            try {
                std::string trimmed;
                trimmed.reserve(str_results[i].size());
                for (char c : str_results[i]) {
                    if (c != ' ' && c != '\n' && c != '\r' && c != '\t') trimmed.push_back(c);
                }
                data_column->append(std::stof(trimmed));
            } catch (const std::exception&) {
                null_column->get_data().back() = 1;
                data_column->append(0.0f);
            }
        } else {
            data_column->append(0.0f);
        }
    }
    return NullableColumn::create(std::move(data_column), std::move(null_column));
}

StatusOr<ColumnPtr> AiFunctions::ai_summarize(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    static const AIChatFuncDesc kDesc{kSystemPromptDefault, kPromptSummarize, nullptr, build_varchar_column, false};
    return execute_descriptored_ai(context, columns, kDesc);
}

StatusOr<ColumnPtr> AiFunctions::ai_complete(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto model_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto prompt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->default_provider();

    std::string extra_params;
    if (columns.size() > 2 && columns[2]->size() > 0) {
        const ColumnPtr& unpacked =
                ColumnHelper::unpack_and_duplicate_const_column(columns[2]->size(), columns[2]);
        const auto* data_col = ColumnHelper::get_data_column(unpacked.get());
        const auto* map_col = dynamic_cast<const MapColumn*>(data_col);
        if (map_col != nullptr && map_col->size() > 0) {
            extra_params = map_column_to_extra_params(*map_col, 0);
        } else {
            VLOG(1) << "[AI] ai_complete: columns[2] is not a MapColumn";
        }
    }
    return execute_chat_function(
            context, columns[0]->size(),
            [&](size_t row) -> std::optional<std::string> {
                if (prompt_viewer.is_null(row) || model_viewer.is_null(row)) return std::nullopt;
                std::string model = model_viewer.value(row).to_string();
                return provider->build_chat_request(model, kSystemPromptDefault,
                                                    prompt_viewer.value(row).to_string(), extra_params);
            },
            build_varchar_column, cfg.endpoint, cfg.api_key);
}

StatusOr<ColumnPtr> AiFunctions::ai_custom_query(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto model_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto prompt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->get_provider(cfg.provider);

    if (cfg.endpoint.empty() || cfg.api_key.empty()) {
        return Status::InvalidArgument(
                "AI RESOURCE function requires 'endpoint' and 'api_key' from AI model resource");
    }

    return execute_chat_function(
            context, columns[0]->size(),
            [&](size_t row) -> std::optional<std::string> {
                if (prompt_viewer.is_null(row) || model_viewer.is_null(row)) return std::nullopt;
                std::string model = model_viewer.value(row).to_string();
                return provider->build_chat_request(model, kSystemPromptDefault,
                                                    prompt_viewer.value(row).to_string(), cfg.extra_params);
            },
            build_varchar_column, cfg.endpoint, cfg.api_key, provider);
}

// ---------------------------------------------------------------------------
// ai_filter — returns BOOLEAN, designed for WHERE clause
// ---------------------------------------------------------------------------

StatusOr<ColumnPtr> AiFunctions::ai_filter(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 2);
    RETURN_IF_ANY_AI_COLUMN_NULL(columns);
    auto text_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto condition_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto size = columns[0]->size();
    const auto& cfg = get_ai_config(context);
    auto* provider = AIProviderRegistry::instance()->default_provider();

    std::vector<std::string> str_results(size);
    std::vector<uint8_t> is_null(size, 0);

    RETURN_IF_ERROR(dispatch_chat_tasks(
            context, size,
            [&](size_t row) -> std::optional<std::string> {
                if (text_viewer.is_null(row) || condition_viewer.is_null(row)) return std::nullopt;
                return provider->build_chat_request(
                        cfg.model, kSystemPromptFilter,
                        strings::Substitute(kPromptFilter, text_viewer.value(row).to_string(),
                                            condition_viewer.value(row).to_string()));
            },
            str_results, is_null, cfg.endpoint, cfg.api_key));

    auto data_column = BooleanColumn::create();
    data_column->reserve(size);
    NullColumnPtr null_column = NullColumn::create();
    null_column->reserve(size);
    for (size_t i = 0; i < size; ++i) {
        null_column->append(is_null[i]);
        if (!is_null[i]) {
            const auto& result = str_results[i];
            std::string lower;
            lower.reserve(result.size());
            for (char c : result) {
                if (c != ' ' && c != '\n' && c != '\r' && c != '\t')
                    lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
            }
            data_column->append((lower == "true" || lower == "yes" || lower == "1") ? 1 : 0);
        } else {
            data_column->append(0);
        }
    }
    return NullableColumn::create(std::move(data_column), std::move(null_column));
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

StatusOr<std::string> AiFunctions::encode_array_arg_to_json(const ColumnPtr& column, size_t row) {
    const ColumnPtr& unpacked = ColumnHelper::unpack_and_duplicate_const_column(column->size(), column);
    const auto* array_col = dynamic_cast<const ArrayColumn*>(ColumnHelper::get_data_column(unpacked.get()));
    if (UNLIKELY(array_col == nullptr)) {
        return Status::InvalidArgument("expected ARRAY column but got incompatible type");
    }
    return encode_array_to_json_string(array_col, row);
}

std::string AiFunctions::encode_array_to_json_string(const ArrayColumn* array_column, size_t row) {
    Datum datum = array_column->get(row);
    const DatumArray& elements = datum.get<DatumArray>();
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    w.StartArray();
    for (const auto& elem : elements) {
        if (elem.is_null()) {
            w.Null();
        } else {
            std::string str_value = elem.get_slice().to_string();
            w.String(str_value.c_str(), static_cast<rapidjson::SizeType>(str_value.size()));
        }
    }
    w.EndArray();
    return {buf.GetString(), buf.GetSize()};
}

ColumnPtr AiFunctions::build_varchar_column(const std::vector<std::string>& results, const std::vector<uint8_t>& is_null,
                                            size_t size) {
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (size_t i = 0; i < size; ++i) {
        if (is_null[i]) {
            result.append_null();
        } else {
            result.append(results[i]);
        }
    }
    return result.build(false);
}

ColumnPtr AiFunctions::build_json_column(const std::vector<std::string>& results, const std::vector<uint8_t>& is_null,
                                         size_t size) {
    auto json_column = JsonColumn::create();
    NullColumnPtr null_column = NullColumn::create();
    null_column->reserve(size);
    for (size_t i = 0; i < size; ++i) {
        if (is_null[i]) {
            null_column->append(1);
            json_column->append_default();
        } else {
            null_column->append(0);
            auto json_value = JsonValue::parse(Slice(results[i]));
            if (json_value.ok()) {
                json_column->append(std::move(json_value.value()));
            } else {
                null_column->get_data().back() = 1;
                json_column->append_default();
            }
        }
    }
    return NullableColumn::create(std::move(json_column), std::move(null_column));
}

} // namespace starrocks
