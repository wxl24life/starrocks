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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.INDEX;

/*
 * ApplyTopNIndexRule enables pushing down TopN operator to scan by using index.
 *
 * This rule optimizes queries with TopN by leveraging external index (e.g., ANN/FTS index)
 * to reduce the amount of data scanned and improve query performance.
 *
 * Pattern:
 *
 *      TopN
 *       |
 *     Scan
 *
 * Transform:
 *
 *      TopN (order by score)              TopN (order by ___score___)
 *       |                                   |
 *     Scan (with score expr)   ->     Scan (with TopNIndexCondition)
 *
 * Key transformations:
 * 1. Push down TopN limit to scan operator using TopNIndexCondition
 * 2. Replace score expression calculation with index score column (___score___)
 * 3. The ANN/FTS index results may contain scores, so we don't need to calculate
 *    the score expression again
 *
 * Conditions:
 * - Only supports single expression in ORDER BY clause
 * - The score expression must be compatible with the index analyzer
 * - Predicate selectivity must be high enough (> 0.001%) to justify using ANN index
 * - TopN limit must not exceed MAX_TOP_N_INDEX_ROWS (10000)
 *
 * @see com.starrocks.connector.index.IndexAnalyzer
 * @see com.starrocks.connector.index.TopNIndexCondition
 */
public class ApplyTopNIndexRule extends TransformationRule {

    public static final String SCORE_COLUMN_NAME = "_INDEX_SCORE";
    // TopN LIMIT alone is often quite small (e.g. LIMIT 10); ANN recall benefits from over-fetching
    // before the post-filter prunes results. Multiplying by 2 trades a little extra index scan
    // work for noticeably better recall stability. Floor at 100 so very small LIMITs still get a
    // reasonable candidate set.
    private static final int LIMIT_TO_LOCAL_N_MULTIPLIER = 2;
    private static final int MIN_LOCAL_N_FROM_LIMIT = 100;

    public static final ApplyTopNIndexRule PAIMON_SCAN =
            new ApplyTopNIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    public ApplyTopNIndexRule(OperatorType type) {
        super(RuleType.TF_APPLY_INDEX_RULE, Pattern.create(OperatorType.LOGICAL_TOPN, type));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        try (Timer ignored = Tracers.watchScope(INDEX, "ApplyTopNIndexRule::check")) {
            return checkImpl(input, context);
        }
    }

    public boolean checkImpl(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator lto = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator lso = (LogicalScanOperator) input.getInputs().get(0).getOp();

        // Check if the scan operator already has an index condition applied.
        // If so, skip this rule to avoid duplicate index pushdown.
        if (lso.getIndexCondition() != null) {
            return false;
        }

        List<Ordering> orderByElements = lto.getOrderByElements();
        // Only one expression is supported for order by.
        // This is a limitation of the current index implementation.
        if (orderByElements.size() != 1) {
            return false;
        }

        // Get the connector metadata for the table.
        // The metadata is required to access index information and capabilities.
        Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(lso.getTable().getCatalogName());
        if (metadata.isEmpty()) {
            return false;
        }

        // Extract the score expression from the ORDER BY clause.
        // The score expression is used to rank results in the index.
        // Projection is required: ORDER BY column ref must be resolvable to its defining expression
        // via the scan's projection map. If the planner ever drops the projection here, fall back
        // safely instead of NPE-ing the optimizer.
        if (lso.getProjection() == null || lso.getProjection().getColumnRefMap() == null) {
            return false;
        }
        Ordering ordering = orderByElements.get(0);
        ScalarOperator scoreExpr = lso.getProjection().getColumnRefMap().get(ordering.getColumnRef());
        if (scoreExpr == null) {
            return false;
        }

        // Create an index analyzer to check if the query can use TopN index.
        // The analyzer evaluates whether the score expression and predicate are compatible with the index.
        IndexAnalyzer indexAnalyzer = new IndexAnalyzer(lso.getTable(), scoreExpr, lso.getPredicate(), metadata.get());
        Pair<Boolean, String> pair = indexAnalyzer.canUseTopNIndex();
        if (!pair.first) {
            return false;
        }

        // top_index_local_rows session variable controls per-shard candidate count:
        //   < 0  : user opted out — skip the global index entirely
        //   == 0 : auto-derive from LIMIT (default; see resolveLocalN)
        //   > 0  : explicit override
        int sessionVar = context.getSessionVariable().getTopIndexLocalRows();
        if (sessionVar < 0) {
            return false;
        }
        if (sessionVar == 0 && !lto.hasLimit()) {
            return false;
        }
        return true;
    }

    private static int resolveLocalN(LogicalTopNOperator lto, OptimizerContext context) {
        int sessionVar = context.getSessionVariable().getTopIndexLocalRows();
        if (sessionVar > 0) {
            return sessionVar;
        }
        // sessionVar == 0: auto-derive from LIMIT (check() guarantees lto.hasLimit()).
        long limit = lto.getLimit();
        long candidate = Math.max(MIN_LOCAL_N_FROM_LIMIT, limit * LIMIT_TO_LOCAL_N_MULTIPLIER);
        return (int) Math.min(candidate, Integer.MAX_VALUE);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        try (Timer ignored = Tracers.watchScope(INDEX, "ApplyTopNIndexRule::transform")) {
            return transformImpl(input, context);
        }
    }

    public List<OptExpression> transformImpl(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator lto = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator lso = (LogicalScanOperator) input.getInputs().get(0).getOp();

        Ordering ordering = lto.getOrderByElements().get(0);
        ScalarOperator scoreExpr = lso.getProjection().getColumnRefMap().get(ordering.getColumnRef());
        Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(lso.getTable().getCatalogName());
        assert metadata.isPresent();

        IndexAnalyzer indexAnalyzer = new IndexAnalyzer(lso.getTable(), scoreExpr, lso.getPredicate(), metadata.get());

        int shard = indexAnalyzer.getShardCount();
        assert shard > 0;

        TopNIndexCondition topNIndexCondition = new TopNIndexCondition();
        // Use prefilter if available to ensure recall
        // Note: Using prefilter may degrade ANN performance, but it's necessary to guarantee
        // that the ANN search doesn't miss relevant results that would be filtered out later.
        // Therefore, we use prefilter whenever it's available, regardless of selectivity.
        topNIndexCondition.setPredicate(indexAnalyzer.getPrefilter());
        topNIndexCondition.setScoreExpr(scoreExpr);

        ScalarOperator postFilterWithoutScore = indexAnalyzer.getPostfilter(true);

        // top_index_local_rows > 0: explicit override. == 0: auto-derive from LIMIT * 2.
        // Each shard is given the same localN value.
        double[] shardId2localN = new double[shard];
        Arrays.fill(shardId2localN, resolveLocalN(lto, context));
        topNIndexCondition.setNLocal(shardId2localN);

        Column scoreColumn = new Column(SCORE_COLUMN_NAME, Type.FLOAT, true);
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        ColumnRefOperator scoreColumnRef = columnRefFactory.create(SCORE_COLUMN_NAME, scoreColumn.getType(), false);

        List<ColumnRefOperator> topNSourceColumns = indexAnalyzer.getTopNSourceColumns();
        List<ColumnRefOperator> erasableColumns = topNSourceColumns.stream().filter(col -> {
            if (lto.getProjection() != null) {
                for (ScalarOperator projectDef : lto.getProjection().getColumnRefMap().values()) {
                    boolean used = projectDef.accept(new ScalarOperatorVisitor<Boolean, Void>() {
                        @Override
                        public Boolean visit(ScalarOperator scalarOperator, Void context) {
                            for (ScalarOperator child : scalarOperator.getChildren()) {
                                if (child.accept(this, context)) {
                                    return true;
                                }
                            }
                            return false;
                        }
                        @Override
                        public Boolean visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                            return col.equals(columnRefOperator);
                        }
                    }, null);
                    if (used) {
                        return false;
                    }
                }
            }
            return true;
        }).collect(Collectors.toList());

        // erase embedding
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = lso.getColRefToColumnMetaMap().entrySet().stream()
                .filter(e -> !erasableColumns.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = lso.getColumnMetaToColRefMap().entrySet().stream()
                .filter(e -> !erasableColumns.contains(e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        ScalarOperator postFilter = indexAnalyzer.getPostfilter(false);
        postFilter = postFilter == null ? null : ScalarOperatorUtil.rewrite(postFilter, scoreExpr, scoreColumnRef);

        colRefToColumnMetaMap.put(scoreColumnRef, scoreColumn);
        columnMetaToColRefMap.put(scoreColumn, scoreColumnRef);

        // Create a new projection that replaces the score expression with the score column
        Projection newProjection = lso.getProjection();
        if (lso.getProjection() != null && lso.getProjection().getColumnRefMap() != null) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = lso.getProjection().getColumnRefMap();
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                // Compare by equals: a downstream rewriter may have copied the score expression
                // into a fresh node, so reference equality misses the projection rewrite.
                if (scoreExpr.equals(entry.getValue())) {
                    newColumnRefMap.put(entry.getKey(), scoreColumnRef);
                } else {
                    newColumnRefMap.put(entry.getKey(), entry.getValue());
                }
            }
            newColumnRefMap.put(scoreColumnRef, scoreColumnRef);
            newProjection = new Projection(newColumnRefMap);
        }

        // Create a new scan operator with TopNIndexCondition pushed down
        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(lso);
        builder.withOperator(lso)
                .setIndexCondition(topNIndexCondition)
                .setColRefToColumnMetaMap(colRefToColumnMetaMap)
                .setColumnMetaToColRefMap(columnMetaToColRefMap)
                .setProjection(newProjection)
                .setPredicate(postFilter);
        if (lso instanceof LogicalPaimonScanOperator) {
            // get/setScanOperatorPredicates declare AnalysisException at the base class level even
            // though LogicalPaimonScanOperator implementations don't actually throw it. Convert to
            // an unchecked rule failure rather than letting it propagate up the rule pipeline.
            try {
                ScanOperatorPredicates scanOperatorPredicates = lso.getScanOperatorPredicates();
                ScanOperatorPredicates newScanOperatorPredicates = new ScanOperatorPredicates();
                newScanOperatorPredicates.getMinMaxColumnRefMap().putAll(scanOperatorPredicates.getMinMaxColumnRefMap());
                List<List<ScalarOperator>> conjunctsList = scanOperatorPredicates.getAllConjunctsList();
                List<List<ScalarOperator>> newConjunctsList = newScanOperatorPredicates.getAllConjunctsList();
                for (int i = 0; i < conjunctsList.size(); i++) {
                    List<ScalarOperator> conjuncts = conjunctsList.get(i);
                    List<ScalarOperator> newConjuncts = conjuncts.stream()
                            .map(it -> ScalarOperatorUtil.rewrite(it, scoreExpr, scoreColumnRef))
                            .collect(Collectors.toList());
                    newConjunctsList.get(i).addAll(newConjuncts);
                }
                ((LogicalPaimonScanOperator.Builder) builder).setScanOperatorPredicates(newScanOperatorPredicates);
            } catch (com.starrocks.common.AnalysisException e) {
                throw new IllegalStateException(
                        "ApplyTopNIndexRule: failed to copy ScanOperatorPredicates for " + lso, e);
            }
        }
        LogicalScanOperator newScanOperator = builder.build();

        // List<OptExpression> newInput = List.of(OptExpression.create(newScanOperator, input.getInputs()));
        List<OptExpression> newInput = List.of(OptExpression.create(newScanOperator));

        // Create a new TopN operator that orders by the score column instead of score expression
        List<Ordering> newOrderByElements = Collections.singletonList(
                new Ordering(scoreColumnRef, ordering.isAscending(), ordering.isNullsFirst()));
        LogicalTopNOperator.Builder ltoBuilder = OperatorBuilderFactory.build(lto);
        ltoBuilder.withOperator(lto).setOrderByElements(newOrderByElements);
        LogicalTopNOperator newTopNOperator = ltoBuilder.build();

        OptExpression project = OptExpression.create(newTopNOperator, newInput);
        return Lists.newArrayList(project);
    }

}
