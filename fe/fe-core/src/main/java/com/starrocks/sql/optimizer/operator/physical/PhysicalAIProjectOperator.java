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

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

/**
 * Physical operator for AI function projections.
 * Inherits all cost/statistics logic from {@link PhysicalProjectOperator};
 * only differs in visitor dispatch so that {@code PlanFragmentBuilder}
 * creates {@code AIProjectNode} instead of {@code ProjectNode}.
 */
public final class PhysicalAIProjectOperator extends PhysicalProjectOperator {

    public PhysicalAIProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                     Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        super(OperatorType.PHYSICAL_AI_PROJECT, columnRefMap, commonSubOperatorMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAIProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalAIProject(optExpression, context);
    }

    @Override
    public String toString() {
        return "PhysicalAIProjectOperator " + getColumnRefMap().keySet();
    }
}
