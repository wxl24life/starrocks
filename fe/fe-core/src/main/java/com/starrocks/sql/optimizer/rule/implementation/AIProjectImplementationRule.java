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

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * Implementation rule that converts a {@link com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator}
 * into a {@link PhysicalAIProjectOperator}.
 * <p>
 * Pattern-matches on {@link OperatorType#LOGICAL_AI_PROJECT} exclusively,
 * so no runtime {@code check()} guard is needed — ordinary projects are
 * naturally excluded by the Pattern.
 */
public class AIProjectImplementationRule extends ImplementationRule {
    public AIProjectImplementationRule() {
        super(RuleType.IMP_AI_PROJECT,
                Pattern.create(OperatorType.LOGICAL_AI_PROJECT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getOp();
        PhysicalAIProjectOperator physicalAIProject = new PhysicalAIProjectOperator(
                projectOperator.getColumnRefMap(),
                Maps.newHashMap());
        return Lists.newArrayList(OptExpression.create(physicalAIProject, input.getInputs()));
    }
}
