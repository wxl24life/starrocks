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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

/**
 * Logical operator for projections that contain AI function calls.
 * <p>
 * Uses {@link OperatorType#LOGICAL_AI_PROJECT} so that Pattern-matching rules
 * naturally distinguish AI projections from ordinary ones — no runtime
 * {@code check()} guards needed in merge / implementation rules.
 * <p>
 * Inherits all data fields and getters from {@link LogicalProjectOperator};
 * only differs in OperatorType and visitor dispatch.
 *
 * <h3>Type identity convention (mirrors the Scan family)</h3>
 * <ul>
 *   <li>{@code operator instanceof LogicalProjectOperator} returns {@code true}
 *       — broad test: "is any projection" (structural checks).</li>
 *   <li>{@code operator.getOpType() == LOGICAL_PROJECT} returns {@code false}
 *       — narrow test: "is a local-only projection" (semantic checks like
 *       {@code isAllSP()} in aggregation push-down).</li>
 * </ul>
 */
public final class LogicalAIProjectOperator extends LogicalProjectOperator {

    public LogicalAIProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        super(OperatorType.LOGICAL_AI_PROJECT, columnRefMap);
    }

    private LogicalAIProjectOperator() {
        super(OperatorType.LOGICAL_AI_PROJECT);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAIProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalAIProject(optExpression, context);
    }

    @Override
    public String toString() {
        return "LogicalAIProjectOperator " + columnRefMap.keySet();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalProjectOperator.Builder {

        @Override
        protected LogicalAIProjectOperator newInstance() {
            return new LogicalAIProjectOperator();
        }

        @Override
        public LogicalAIProjectOperator build() {
            return (LogicalAIProjectOperator) super.build();
        }

        @Override
        public Builder withOperator(LogicalProjectOperator operator) {
            super.withOperator(operator);
            return this;
        }

        @Override
        public Builder setLimit(long limit) {
            super.setLimit(limit);
            return this;
        }

        @Override
        public Builder setPredicate(ScalarOperator predicate) {
            super.setPredicate(predicate);
            return this;
        }

        @Override
        public Builder setColumnRefMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            builder.columnRefMap = columnRefMap;
            return this;
        }

        @Override
        public Builder setProjection(Projection projection) {
            throw new UnsupportedOperationException("Shouldn't set projection to AI Project Operator");
        }
    }
}
