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

package com.starrocks.connector.index;

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.EQ;
import static com.starrocks.analysis.BinaryType.GE;
import static com.starrocks.analysis.BinaryType.GT;
import static com.starrocks.analysis.BinaryType.LE;
import static com.starrocks.analysis.BinaryType.LT;
import static com.starrocks.analysis.BinaryType.NE;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class IndexAnalyzer {

    private static final Set<String> APPROX_VECTOR_DISTANCE_FUNCTIONS = Set.of(
            FunctionSet.APPROX_COSINE_SIMILARITY,
            FunctionSet.APPROX_INNER_PRODUCT,
            FunctionSet.APPROX_L2_DISTANCE
    );

    private static final Set<String> FTS_CALL_FUNCTIONS = Set.of(
            "match_prefix",
            "match_wildcard"
    );

    private record BinaryPredicateIndexSpec(Table.TableType tableType, String indexName,
                                            List<BinaryType> supportedBinaryTypes) {
    }

    private record VectorIndexSpec(Table.TableType tableType, String indexName) {
    }

    private static final List<BinaryPredicateIndexSpec> BINARY_PREDICATE_INDEX_SUPPORT = List.of(
            new BinaryPredicateIndexSpec(Table.TableType.PAIMON, "bitmap", Arrays.asList(EQ, NE)),
            new BinaryPredicateIndexSpec(Table.TableType.PAIMON, "bsi", Arrays.asList(EQ, NE, LT, LE, GT, GE))
    );

    private static final List<VectorIndexSpec> VECTOR_INDEX_SUPPORT = List.of(
            new VectorIndexSpec(Table.TableType.PAIMON, "lumina"),
            // Backward-compat with legacy LuminaVectorGlobalIndexerFactory identifier; Paimon docs
            // explicitly keep accepting this identifier for existing tables.
            new VectorIndexSpec(Table.TableType.PAIMON, "lumina-vector-ann")
    );

    private record FTSIndexSpec(Table.TableType tableType, String indexName) {
    }

    private static final List<FTSIndexSpec> FTS_INDEX_SUPPORT = List.of(
            new FTSIndexSpec(Table.TableType.PAIMON, "lucene"),
            new FTSIndexSpec(Table.TableType.PAIMON, "lucene-fts"),
            new FTSIndexSpec(Table.TableType.PAIMON, "tantivy-fts"),
            new FTSIndexSpec(Table.TableType.PAIMON, "tantivy-fulltext")
    );

    private final Table table;
    private final ScalarOperator filter;
    private final ScalarOperator scoreExpr;
    private final ConnectorMetadata metadata;
    private Map<String, Set<String>> columnIndexes;
    private List<ScalarOperator> conjuncts;
    private List<ColumnRefOperator> topNSourceColumns;

    // Check if the expression is of the form [column] [op] [literal]
    private static Optional<String> checkBinaryPredicateAndGetColumnName(BinaryPredicateOperator predicate) {
        ScalarOperator lhs = predicate.getChild(0);
        ScalarOperator rhs = predicate.getChild(1);
        if (lhs instanceof ColumnRefOperator && ScalarOperatorUtil.isLiteral(rhs)) {
            String name = ((ColumnRefOperator) lhs).getName();
            return Optional.of(name);
        }
        return Optional.empty();
    }

    enum PredicateStageType {
        Prefilter,
        Postfilter,
        ScoreFilter,
    }

    private final ScalarOperatorVisitor<PredicateStageType, Void> predicateIndexCheckVisitor = new ScalarOperatorVisitor<>() {
        @Override
        public PredicateStageType visitBinaryPredicate(BinaryPredicateOperator predicate, Void ignore) {
            Optional<String> columnNameOpt = checkBinaryPredicateAndGetColumnName(predicate);
            if (columnNameOpt.isPresent()) {
                Set<String> indexNames = columnIndexes.get(columnNameOpt.get());
                if (indexNames != null) {
                    for (String indexName : indexNames) {
                        for (BinaryPredicateIndexSpec spec : BINARY_PREDICATE_INDEX_SUPPORT) {
                            if (table.getType() == spec.tableType() &&
                                    spec.indexName().equalsIgnoreCase(indexName) &&
                                    spec.supportedBinaryTypes().contains(predicate.getBinaryType())) {
                                return PredicateStageType.Prefilter;
                            }
                        }
                    }
                }
            }
            if (predicate.getBinaryType() == GE || predicate.getBinaryType() == GT) {
                if (predicate.getChild(1) instanceof ConstantOperator) {
                    if (ScalarOperatorUtil.isEquivalentIgnoreCast(predicate.getChild(0), scoreExpr)) {
                        return PredicateStageType.ScoreFilter;
                    }
                }
            }
            // todo: check score filter
            return PredicateStageType.Postfilter;
        }

        @Override
        public PredicateStageType visitMatchExprOperator(MatchExprOperator predicate, Void ignore) {
            ScalarOperator child0 = predicate.getChild(0);
            if (child0 instanceof ColumnRefOperator) {
                String columnName = ((ColumnRefOperator) child0).getName();
                Set<String> indexNames = columnIndexes.get(columnName);
                if (indexNames != null) {
                    for (String indexName : indexNames) {
                        for (FTSIndexSpec spec : FTS_INDEX_SUPPORT) {
                            if (table.getType() == spec.tableType() &&
                                    spec.indexName().equalsIgnoreCase(indexName)) {
                                return PredicateStageType.Prefilter;
                            }
                        }
                    }
                }
            }
            return PredicateStageType.Postfilter;
        }

        @Override
        public PredicateStageType visitCall(CallOperator call, Void ignore) {
            String fnName = call.getFnName();
            if (FTS_CALL_FUNCTIONS.contains(fnName.toLowerCase())) {
                ScalarOperator arg0 = call.getChild(0);
                if (arg0 instanceof ColumnRefOperator) {
                    String columnName = ((ColumnRefOperator) arg0).getName();
                    Set<String> indexNames = columnIndexes.get(columnName);
                    if (indexNames != null) {
                        for (String indexName : indexNames) {
                            for (FTSIndexSpec spec : FTS_INDEX_SUPPORT) {
                                if (table.getType() == spec.tableType() &&
                                        spec.indexName().equalsIgnoreCase(indexName)) {
                                    return PredicateStageType.Prefilter;
                                }
                            }
                        }
                    }
                }
            }
            return PredicateStageType.Postfilter;
        }

        @Override
        public PredicateStageType visit(ScalarOperator scalarOperator, Void ignore) {
            return PredicateStageType.Postfilter;
        }
    };

    // currently only support approx_vector_distance(embedding, array literal)
    private final ScalarOperatorVisitor<Boolean, Void> topNIndexCheckVisitor = new ScalarOperatorVisitor<>() {
        @Override
        public Boolean visitCall(CallOperator call, Void context) {
            if (APPROX_VECTOR_DISTANCE_FUNCTIONS.contains(call.getFnName())) {
                boolean isLhsColumn = call.getChild(0) instanceof ColumnRefOperator;
                ScalarOperator column = call.getChild(isLhsColumn ? 0 : 1);
                ScalarOperator array = call.getChild(isLhsColumn ? 1 : 0);
                if (column instanceof ColumnRefOperator &&
                        ScalarOperatorUtil.isFloatArray(array) &&
                        ScalarOperatorUtil.isLiteral(array)) {
                    String name = ((ColumnRefOperator) column).getName();
                    // ORDER BY column may have no index built (other columns may); guard before iterate.
                    Set<String> indexNames = columnIndexes.get(name);
                    if (indexNames == null) {
                        return false;
                    }
                    for (String indexName : indexNames) {
                        for (VectorIndexSpec spec : VECTOR_INDEX_SUPPORT) {
                            if (table.getType() == spec.tableType() && indexName.equals(spec.indexName())) {
                                topNSourceColumns.add((ColumnRefOperator) column);
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }
    };

    public IndexAnalyzer(
            Table table,
            ScalarOperator scoreExpr,
            ScalarOperator filter,
            ConnectorMetadata metadata
    ) {
        this.table = table;
        this.scoreExpr = scoreExpr;
        this.filter = filter;
        this.metadata = metadata;
    }

    public IndexAnalyzer(Table table, ScalarOperator predicate, ConnectorMetadata metadata) {
        this(table, null, predicate, metadata);
    }

    public IndexAnalyzer(Table table, ConnectorMetadata metadata) {
        this(table, null, null, metadata);
    }

    private void setupColumnIndexes() {
        if (columnIndexes == null) {
            if (FeConstants.runningUnitTest) {
                // for UT
                columnIndexes = new HashMap<>();
                if (table instanceof PaimonTable) {
                    Map<String, String> options = ((PaimonTable) table).getNativeTable().options();
                    for (Map.Entry<String, String> e : options.entrySet()) {
                        String[] split = e.getKey().replace("'", "").split("\\.");
                        if (split.length == 3 &&
                                split[0].equalsIgnoreCase("global-index") &&
                                split[2].equals("columns")) {
                            String indexType = split[1].toLowerCase();
                            for (String column : e.getValue().replace("'", "").split(",")) {
                                columnIndexes.computeIfAbsent(column, k -> new HashSet<>()).add(indexType);
                            }
                        }
                    }
                }
                return;
            }
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "ScanIndexFileEntries")) {
                columnIndexes = metadata.getGlobalIndexes(table);
            }
        }
    }

    private void decomposeConjuncts() {
        if (conjuncts == null) {
            conjuncts = Utils.extractConjuncts(filter);
        }
    }

    // Check if the order by operation hits the topN index.
    public Pair<Boolean, String> canUseTopNIndex() {
        if (scoreExpr == null) {
            return Pair.create(false, "scoreExpr is null");
        }
        setupColumnIndexes();
        if (columnIndexes == null || columnIndexes.isEmpty()) {
            return Pair.create(false, "columnIndexes is empty");
        }
        if (topNSourceColumns == null) {
            topNSourceColumns = new ArrayList<>();
            scoreExpr.accept(topNIndexCheckVisitor, null);
        }
        if (topNSourceColumns.isEmpty()) {
            return Pair.create(false, "The ORDER BY clause did not hit the topN index.");
        }
        if (!FeConstants.runningUnitTest && !metadata.checkGlobalIndexAvailable(table)) {
            return Pair.create(false, "The global index is not available, please build the index using Spark first.");
        }
        return Pair.create(true, null);
    }

    public boolean canUsePredicateIndex() {
        // get global indexes meta
        setupColumnIndexes();
        if (columnIndexes == null || columnIndexes.isEmpty()) {
            return false;
        }

        decomposeConjuncts();
        boolean check = false;
        for (ScalarOperator predicate : conjuncts) {
            check = predicate.accept(predicateIndexCheckVisitor, null) == PredicateStageType.Prefilter;
            if (check) {
                break;
            }
        }

        // check index available
        return check && (metadata.checkGlobalIndexAvailable(table) || FeConstants.runningUnitTest);
    }

    public List<ColumnRefOperator> getTopNSourceColumns() {
        if (scoreExpr == null) {
            return new ArrayList<>();
        }
        setupColumnIndexes();
        if (columnIndexes == null || columnIndexes.isEmpty()) {
            return new ArrayList<>();
        }
        if (topNSourceColumns == null) {
            topNSourceColumns = new ArrayList<>();
            scoreExpr.accept(topNIndexCheckVisitor, null);
        }
        return topNSourceColumns;
    }

    public ScalarOperator getPrefilter() {
        setupColumnIndexes();
        if (columnIndexes == null || columnIndexes.isEmpty()) {
            return null;
        }

        decomposeConjuncts();
        List<ScalarOperator> conjuncts = this.conjuncts.stream()
                .filter(p -> p.accept(predicateIndexCheckVisitor, null) ==
                        PredicateStageType.Prefilter)
                .collect(Collectors.toList());
        return conjuncts.isEmpty() ? null :
                (conjuncts.size() == 1 ? conjuncts.get(0) : ScalarOperatorUtil.and(conjuncts));
    }

    public ScalarOperator getPostfilter(boolean withoutScore) {
        setupColumnIndexes();
        if (columnIndexes == null || columnIndexes.isEmpty()) {
            return filter;
        }
        decomposeConjuncts();
        List<ScalarOperator> conjuncts = this.conjuncts.stream()
                .filter(p -> {
                    PredicateStageType type = p.accept(predicateIndexCheckVisitor, null);
                    if (withoutScore) {
                        return type == PredicateStageType.Postfilter;
                    }
                    return type == PredicateStageType.Postfilter ||
                            type == PredicateStageType.ScoreFilter;
                })
                .collect(Collectors.toList());
        return conjuncts.isEmpty() ? null :
                (conjuncts.size() == 1 ? conjuncts.get(0) : ScalarOperatorUtil.and(conjuncts));
    }

    public ScalarOperator getFilterWithoutScore() {
        decomposeConjuncts();
        List<ScalarOperator> conjuncts = this.conjuncts.stream()
                .filter(p -> p.accept(predicateIndexCheckVisitor, null) !=
                        PredicateStageType.ScoreFilter)
                .collect(Collectors.toList());
        return conjuncts.isEmpty() ? null :
                (conjuncts.size() == 1 ? conjuncts.get(0) : ScalarOperatorUtil.and(conjuncts));
    }

    // get global index shard count
    public int getShardCount() {
        return FeConstants.runningUnitTest ? 2 : metadata.getGlobalIndexShardCount(table);
    }

    public <T> List<T> getIndexShards(Function<Object, T> mapper) {
        return metadata.getGlobalIndexShards(table, mapper);
    }

}

