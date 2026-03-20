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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PruneGroupByKeysRuleTest {

    @Test
    public void testSkipsWhenGroupingKeyMissingFromProjections(
            @Mocked OptimizerContext optimizerContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator col1 = columnRefFactory.create("col1", IntegerType.INT, false);
        ColumnRefOperator col2 = columnRefFactory.create("col2", VarcharType.VARCHAR, false);
        ColumnRefOperator col3 = columnRefFactory.create("col3", DateType.DATE, false);

        Map<ColumnRefOperator, ScalarOperator> projections = new HashMap<>();
        projections.put(col1, col1);
        projections.put(col2, col2);

        LogicalProjectOperator projectOp = new LogicalProjectOperator(projections);
        OptExpression leafExpr = OptExpression.create(
                new LogicalValuesOperator(Lists.newArrayList(col1, col2), Collections.emptyList()));
        OptExpression projectExpr = OptExpression.create(projectOp, leafExpr);

        List<ColumnRefOperator> groupingKeys = Lists.newArrayList(col1, col2, col3);
        LogicalAggregationOperator aggOp = new LogicalAggregationOperator.Builder()
                .setType(AggType.GLOBAL)
                .setGroupingKeys(groupingKeys)
                .setPartitionByColumns(groupingKeys)
                .setAggregations(ImmutableMap.of())
                .build();
        OptExpression aggExpr = OptExpression.create(aggOp, projectExpr);

        new Expectations() {
            {
                optimizerContext.getColumnRefFactory();
                result = columnRefFactory;
                minTimes = 0;
            }
        };

        PruneGroupByKeysRule rule = new PruneGroupByKeysRule();
        List<OptExpression> result = rule.transform(aggExpr, optimizerContext);

        Assertions.assertTrue(result.isEmpty(), "Should skip when grouping key missing from projections");
    }

    @Test
    public void testWorksNormallyWhenAllGroupingKeysPresent(
            @Mocked OptimizerContext optimizerContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator col1 = columnRefFactory.create("col1", IntegerType.INT, false);
        ColumnRefOperator col2 = columnRefFactory.create("col2", IntegerType.INT, false);
        ColumnRefOperator inputCol1 = columnRefFactory.create("input1", IntegerType.INT, false);

        Map<ColumnRefOperator, ScalarOperator> projections = new HashMap<>();
        projections.put(col1, inputCol1);
        projections.put(col2, inputCol1);

        LogicalProjectOperator projectOp = new LogicalProjectOperator(projections);
        OptExpression leafExpr = OptExpression.create(
                new LogicalValuesOperator(Lists.newArrayList(inputCol1), Collections.emptyList()));
        OptExpression projectExpr = OptExpression.create(projectOp, leafExpr);

        List<ColumnRefOperator> groupingKeys = Lists.newArrayList(col1, col2);
        ColumnRefOperator aggOutput = columnRefFactory.create("count", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> aggs = new HashMap<>();
        aggs.put(aggOutput, new CallOperator("count", IntegerType.BIGINT, Collections.emptyList()));

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator.Builder()
                .setType(AggType.GLOBAL)
                .setGroupingKeys(groupingKeys)
                .setPartitionByColumns(groupingKeys)
                .setAggregations(aggs)
                .build();
        OptExpression aggExpr = OptExpression.create(aggOp, projectExpr);

        new Expectations() {
            {
                optimizerContext.getColumnRefFactory();
                result = columnRefFactory;
                minTimes = 0;
            }
        };

        PruneGroupByKeysRule rule = new PruneGroupByKeysRule();
        List<OptExpression> result = rule.transform(aggExpr, optimizerContext);

        Assertions.assertFalse(result.isEmpty(), "Should transform when duplicate grouping keys exist");
    }
}
