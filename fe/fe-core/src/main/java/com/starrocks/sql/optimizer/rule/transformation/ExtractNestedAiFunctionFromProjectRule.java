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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * The sole Project-side entry point for creating {@code LogicalAIProjectOperator} nodes.
 * (The only other entry point is {@code ExtractAiFunctionFromFilterRule} for Filter-side.)
 *
 * <p>Extracts AI function calls from project expressions into dedicated
 * AIProject nodes below the current one, with common sub-expression
 * elimination (CSE) across all expressions.
 *
 * <p>This handles three cases:
 * <ol>
 *   <li>Nested AI calls: {@code length(ai_sentiment(col))} —
 *       the inner AI call is extracted to an AIProject.</li>
 *   <li>Root-level AI calls: {@code ai_sentiment(col)} —
 *       extracted identically to nested calls.</li>
 *   <li>Duplicate AI calls: {@code ai_sentiment(col), length(ai_sentiment(col))} —
 *       the two identical calls (by {@code equals()}) share a single ColumnRef,
 *       so the AI HTTP call happens only once.</li>
 * </ol>
 *
 * <p>Multi-level nesting (e.g. {@code ai_translate(ai_fix_grammar(ai_summarize(col)))})
 * is fully decomposed in a single {@code transform()} call by iteratively peeling
 * off the innermost AI layer until the outer expressions contain no AI calls.
 *
 * <pre>
 * Before:
 *   Project(slot4 = ai_translate(ai_fix_grammar(ai_summarize(col))))
 *
 * After:
 *   Project(slot4 = ai_translate(ref_b))
 *     → AIProject(ref_b = ai_fix_grammar(ref_a))
 *       → AIProject(ref_a = ai_summarize(col), col = col)
 * </pre>
 */
public class ExtractNestedAiFunctionFromProjectRule extends TransformationRule {

    public ExtractNestedAiFunctionFromProjectRule() {
        super(RuleType.TF_EXTRACT_NESTED_AI_FUNCTION_FROM_PROJECT,
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator project = (LogicalProjectOperator) input.getOp();
        for (ScalarOperator expr : project.getColumnRefMap().values()) {
            if (Utils.hasAiFunction(expr)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> currentMap =
                ((LogicalProjectOperator) input.getOp()).getColumnRefMap();
        long limit = ((LogicalProjectOperator) input.getOp()).getLimit();
        List<OptExpression> childInputs = input.getInputs();

        // Pass-through columns from the original child; updated each iteration
        // to include newly created AI ColumnRefs.
        ColumnRefSet passThrough = new ColumnRefSet();
        for (int colId : input.inputAt(0).getOutputColumns().getColumnIds()) {
            passThrough.union(colId);
        }

        OptExpression bottom = null;

        while (true) {
            Map<CallOperator, ColumnRefOperator> aiDedup = new HashMap<>();
            IdentityHashMap<CallOperator, ColumnRefOperator> replaceMap = new IdentityHashMap<>();

            for (ScalarOperator expr : currentMap.values()) {
                for (CallOperator aiCall : Utils.collectAiFunctions(expr)) {
                    ColumnRefOperator ref = aiDedup.get(aiCall);
                    if (ref == null) {
                        ref = context.getColumnRefFactory().create(
                                aiCall, aiCall.getType(), aiCall.isNullable());
                        aiDedup.put(aiCall, ref);
                    }
                    replaceMap.put(aiCall, ref);
                }
            }

            if (aiDedup.isEmpty()) {
                break;
            }

            Map<ColumnRefOperator, ScalarOperator> innerMap = Maps.newHashMap();
            for (int colId : passThrough.getColumnIds()) {
                ColumnRefOperator ref = context.getColumnRefFactory().getColumnRef(colId);
                innerMap.put(ref, ref);
            }
            for (Map.Entry<CallOperator, ColumnRefOperator> e : aiDedup.entrySet()) {
                innerMap.put(e.getValue(), e.getKey());
            }

            List<OptExpression> aiInputs = (bottom != null)
                    ? Lists.newArrayList(bottom) : childInputs;
            bottom = OptExpression.create(new LogicalAIProjectOperator(innerMap), aiInputs);

            // Update pass-through to include the new AI ColumnRefs for the next layer
            for (ColumnRefOperator ref : aiDedup.values()) {
                passThrough.union(ref.getId());
            }

            Map<ColumnRefOperator, ScalarOperator> replacedMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : currentMap.entrySet()) {
                replacedMap.put(e.getKey(), Utils.replaceAiFunctions(e.getValue(), replaceMap));
            }
            currentMap = replacedMap;
        }

        if (bottom == null) {
            return Lists.newArrayList();
        }

        OptExpression outerExpr = OptExpression.create(
                new LogicalProjectOperator(currentMap, limit), bottom);
        return Lists.newArrayList(outerExpr);
    }
}
