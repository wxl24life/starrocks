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
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class ApplyPredicateIndexRule extends TransformationRule {

    private static final Logger LOG = LoggerFactory.getLogger(ApplyPredicateIndexRule.class);

    public static final ApplyPredicateIndexRule PAIMON_SCAN =
            new ApplyPredicateIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    public ApplyPredicateIndexRule(OperatorType type) {
        super(RuleType.TF_APPLY_INDEX_RULE, Pattern.create(type));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalScanOperator lso = (LogicalScanOperator) input.getOp();
        if (lso.getIndexCondition() != null) {
            return false;
        }
        Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(lso.getTable().getCatalogName());
        if (metadata.isEmpty()) {
            LOG.debug("metadata is empty for catalog {}", lso.getTable().getCatalogName());
            return false;
        }
        IndexAnalyzer indexAnalyzer = new IndexAnalyzer(lso.getTable(), lso.getPredicate(), metadata.get());
        return indexAnalyzer.canUsePredicateIndex();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {

        LogicalScanOperator lso = (LogicalScanOperator) input.getOp();

        Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(lso.getTable().getCatalogName());
        if (metadata.isEmpty()) {
            throw new RuntimeException("metadata is empty");
        }

        IndexAnalyzer indexAnalyzer = new IndexAnalyzer(lso.getTable(), lso.getPredicate(), metadata.get());
        ScalarOperator prefilter = indexAnalyzer.getPrefilter();
        ScalarOperator postfilter = indexAnalyzer.getPostfilter(true);

        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(lso);
        LogicalScanOperator newScanOperator = (LogicalScanOperator) builder.withOperator(lso)
                .setIndexCondition(new IndexCondition(prefilter))
                .setPredicate(postfilter)
                .build();

        return Lists.newArrayList(new OptExpression(newScanOperator));
    }
}
