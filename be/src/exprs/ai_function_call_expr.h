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

#include <string>

#include "common/object_pool.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"

namespace starrocks {

// Dedicated expression class for AI functions (TFunctionBinaryType::AI).
//
// AI functions involve blocking HTTP calls to LLM services, so they must be
// executed on the ScanExecutor IO thread pool via AIChunkSource rather than
// on the pipeline driver thread. evaluate_checked() returns InternalError to
// enforce this invariant; AIChunkSource calls execute_for_operator() directly.
//
// Execution flow: AIScanOperator spawns a bthread that calls
// execute_for_operator() → dispatch() → bthread::CV yields the pthread
// during HTTP wait. No async state machine needed.
class AIFunctionCallExpr final : public Expr {
public:
    explicit AIFunctionCallExpr(const TExprNode& node);

    ~AIFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new AIFunctionCallExpr(*this)); }

    bool is_constant() const override { return false; }

    int fn_context_index() const { return _fn_context_index; }

    const std::string& ai_model_config_id() const { return _ai_model_config_id; }

    // Synchronous entry point for AIChunkSource (runs on bthread).
    // Blocks until all HTTP calls complete (bthread::CV yields the pthread).
    StatusOr<ColumnPtr> execute_for_operator(ExprContext* context, Chunk* ptr);

protected:
    [[nodiscard]] Status prepare(RuntimeState* state, ExprContext* context) override;

    [[nodiscard]] Status open(RuntimeState* state, ExprContext* context,
                              FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    [[nodiscard]] StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    StatusOr<ColumnPtr> _call_scalar_function(ExprContext* context, Chunk* ptr);

    const FunctionDescriptor* _fn_desc{nullptr};
    // Config key into TAIProjectNode.ai_model_configs ("__system__" or resource name)
    std::string _ai_model_config_id;
};

} // namespace starrocks
