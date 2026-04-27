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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extract AI function calls from filter predicates into a project node below the filter.
 *
 * Before:
 *   Filter(predicate contains ai_gen(...) etc.)
 *     -> child
 *
 * After:
 *   Filter(predicate with ai_gen(...) replaced by column ref)
 *     -> Project(pass-through + new_col_ref = ai_gen(...))
 *       -> child
 *
 * This ensures AI functions are materialized via AIProjectNode (async IO)
 * before the filter evaluates the predicate, avoiding blocking execution.
 */
public class ExtractAiFunctionFromFilterRule extends TransformationRule {

    public ExtractAiFunctionFromFilterRule() {
        super(RuleType.TF_EXTRACT_AI_FUNCTION_FROM_FILTER,
                Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        return Utils.hasAiFunction(filter.getPredicate());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filter.getPredicate();

        // Split conjuncts into those that reference AI functions and those that don't.
        // Non-AI predicates (e.g. id <= 10000) should be pushed below the AIProject
        // so that fewer rows are sent to the LLM.
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> aiConjuncts = Lists.newArrayList();
        List<ScalarOperator> nonAiConjuncts = Lists.newArrayList();
        for (ScalarOperator conj : conjuncts) {
            if (Utils.hasAiFunction(conj)) {
                aiConjuncts.add(conj);
            } else {
                nonAiConjuncts.add(conj);
            }
        }

        // De-duplicate by identity (same CallOperator instance may appear multiple times via conjuncts)
        IdentityHashMap<CallOperator, ColumnRefOperator> aiToColRef = new IdentityHashMap<>();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();

        // Pass through all columns from the child
        for (int colId : input.inputAt(0).getOutputColumns().getColumnIds()) {
            ColumnRefOperator ref = context.getColumnRefFactory().getColumnRef(colId);
            projectMap.put(ref, ref);
        }

        // Create a new column ref for each unique AI function call
        for (ScalarOperator aiConj : aiConjuncts) {
            for (CallOperator aiCall : Utils.collectAiFunctions(aiConj)) {
                if (!aiToColRef.containsKey(aiCall)) {
                    ColumnRefOperator newRef = context.getColumnRefFactory().create(
                            aiCall, aiCall.getType(), aiCall.isNullable());
                    aiToColRef.put(aiCall, newRef);
                    projectMap.put(newRef, aiCall);
                }
            }
        }

        // Build the plan bottom-up:
        //   child -> [non-AI Filter] -> AIProject -> [AI Filter]
        OptExpression childExpr = input.inputAt(0);

        // Push non-AI predicates below the AIProject to reduce rows sent to LLM
        if (!nonAiConjuncts.isEmpty()) {
            ScalarOperator pushdownPred = Utils.compoundAnd(nonAiConjuncts);
            childExpr = OptExpression.create(new LogicalFilterOperator(pushdownPred), childExpr);
        }

        LogicalAIProjectOperator projectOp = new LogicalAIProjectOperator(projectMap);
        OptExpression projectExpr = OptExpression.create(projectOp, childExpr);

        // Replace AI function calls in the AI conjuncts with column refs
        ScalarOperator aiPredicate = Utils.compoundAnd(aiConjuncts);
        ScalarOperator newAiPredicate = Utils.replaceAiFunctions(aiPredicate, aiToColRef);
        OptExpression filterExpr = OptExpression.create(new LogicalFilterOperator(newAiPredicate), projectExpr);

        return Lists.newArrayList(filterExpr);
    }
}
