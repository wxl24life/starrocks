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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Serialize a {@link ScalarOperator} tree to JSON (for cross-tier transport, e.g. the predicate
 * payload sent to the Paimon Global Index reader on BE) or to a SQL-like string (for debug logs
 * and trace dumps).
 *
 * <p>Single-character JSON keys keep the predicate payload compact since BE indexes may receive
 * deeply nested expressions.
 */
public class ScalarOperatorSerializer {

    public static final String ARGUMENTS = "a";
    public static final String BINARY_TYPE = "b";
    public static final String CHILDREN = "c";
    public static final String COMPOUND_TYPE = "ct";
    public static final String FN_NAME = "f";
    public static final String ITEM_TYPE = "i";
    public static final String NAME = "n";
    public static final String OPERATOR_TYPE = "o";
    public static final String TYPE = "t";
    public static final String VALUE = "v";

    public static final Map<Class<?>, String> OPERATOR_TYPES = Map.of(
            ArrayOperator.class, "a",
            BinaryPredicateOperator.class, "b",
            CallOperator.class, "ca",
            ConstantOperator.class, "co",
            CompoundPredicateOperator.class, "cp",
            ColumnRefOperator.class, "cr"
    );

    // ANN distance functions are symmetric in their two arguments, so writing
    // `approx_cosine_similarity([query], col)` and `approx_cosine_similarity(col, [query])`
    // are semantically equivalent. The BE Paimon Global Index TopN evaluator,
    // however, only knows how to read (column_ref, query_vector) order — it dereferences
    // arguments[0] as the column NAME field. Normalize here so the JSON sent to BE is
    // always column-first, regardless of how the user wrote the SQL.
    private static final Set<String> ANN_FUNCTIONS = Set.of(
            FunctionSet.APPROX_COSINE_SIMILARITY,
            FunctionSet.APPROX_INNER_PRODUCT,
            FunctionSet.APPROX_L2_DISTANCE
    );

    private ScalarOperatorSerializer() {
    }

    /**
     * If `call` is an ANN function and the column ref is the second argument, return a
     * (column_ref, other) view. Otherwise return the original arguments unchanged.
     * Does NOT mutate the input CallOperator.
     */
    private static List<ScalarOperator> normalizeAnnArgs(CallOperator call) {
        List<ScalarOperator> args = call.getArguments();
        if (!ANN_FUNCTIONS.contains(call.getFnName()) || args.size() != 2) {
            return args;
        }
        if (args.get(0) instanceof ColumnRefOperator) {
            return args;
        }
        if (args.get(1) instanceof ColumnRefOperator) {
            return List.of(args.get(1), args.get(0));
        }
        return args;
    }

    public static Object toJson(ScalarOperator x) {
        if (x == null) {
            return null;
        }
        return x.accept(new ScalarOperatorVisitor<Map<String, Object>, Void>() {
            @Override
            public Map<String, Object> visit(ScalarOperator scalarOperator, Void context) {
                throw new RuntimeException("not support " + scalarOperator.getClass());
            }

            @Override
            public Map<String, Object> visitArray(ArrayOperator array, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(array.getClass()));
                Type itemType = ((ArrayType) array.getType()).getItemType();
                map.put(ITEM_TYPE, itemType.toSql());
                map.put(CHILDREN, array.getChildren().stream()
                        .map(it -> it.accept(this, null))
                        .collect(Collectors.toList()));
                return map;
            }

            @Override
            public Map<String, Object> visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(predicate.getClass()));
                map.put(BINARY_TYPE, predicate.getBinaryType().name());
                map.put(CHILDREN, predicate.getChildren().stream()
                        .map(it -> it.accept(this, null))
                        .collect(Collectors.toList()));
                return map;
            }

            @Override
            public Map<String, Object> visitCall(CallOperator call, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(call.getClass()));
                map.put(FN_NAME, call.getFnName());
                List<ScalarOperator> args = normalizeAnnArgs(call);
                map.put(ARGUMENTS, args.stream()
                        .map(it -> it.accept(this, null))
                        .collect(Collectors.toList()));
                return map;
            }

            @Override
            public Map<String, Object> visitCastOperator(CastOperator operator, Void context) {
                Map<String, Object> map = operator.getChild(0).accept(this, null);
                Type type = operator.getType();
                if (type instanceof ArrayType) {
                    map.put(ITEM_TYPE, ((ArrayType) type).getItemType().toTypeString());
                } else {
                    map.put(TYPE, type.toTypeString());
                }
                return map;
            }

            @Override
            public Map<String, Object> visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(predicate.getClass()));
                map.put(COMPOUND_TYPE, predicate.getCompoundType().name());
                map.put(CHILDREN, predicate.getChildren().stream()
                        .map(it -> it.accept(this, null))
                        .collect(Collectors.toList()));
                return map;
            }

            @Override
            public Map<String, Object> visitConstant(ConstantOperator literal, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(literal.getClass()));
                map.put(TYPE, literal.getType().toTypeString());
                map.put(VALUE, literal.getValue());
                return map;
            }

            @Override
            public Map<String, Object> visitVariableReference(ColumnRefOperator variable, Void context) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(OPERATOR_TYPE, OPERATOR_TYPES.get(variable.getClass()));
                map.put(TYPE, variable.getType().toTypeString());
                map.put(NAME, variable.getName());
                return map;
            }
        }, null);
    }

    public static String toSql(ScalarOperator scalarOperator) {
        return scalarOperator.accept(new ScalarOperatorVisitor<String, Void>() {
            @Override
            public String visit(ScalarOperator scalarOperator, Void context) {
                throw new RuntimeException("[AQP] not support: " + scalarOperator.getClass().getName());
            }

            @Override
            public String visitInPredicate(InPredicateOperator predicate, Void context) {
                return String.format("%s in (%s)",
                        toSql(predicate.getChild(0)),
                        predicate.getChildren().subList(1, predicate.getChildren().size()).stream()
                                .map(ScalarOperatorSerializer::toSql)
                                .collect(Collectors.joining(", ")));
            }

            @Override
            public String visitArray(ArrayOperator array, Void context) {
                return String.format("[%s]",
                        array.getChildren().stream()
                                .map(ScalarOperatorSerializer::toSql)
                                .collect(Collectors.joining(", ")));
            }

            @Override
            public String visitCall(CallOperator call, Void context) {
                return String.format("%s(%s)",
                        call.getFnName(),
                        call.getChildren().stream()
                                .map(ScalarOperatorSerializer::toSql)
                                .collect(Collectors.joining(", ")));
            }

            @Override
            public String visitCastOperator(CastOperator operator, Void context) {
                return String.format("cast(%s as %s)",
                        toSql(operator.getChild(0)),
                        operator.getType());
            }

            @Override
            public String visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
                return String.format("(%s) %s (%s)",
                        toSql(predicate.getChild(0)),
                        predicate.getCompoundType(),
                        toSql(predicate.getChild(1)));
            }

            @Override
            public String visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                return String.format("%s %s %s",
                        toSql(predicate.getChild(0)),
                        predicate.getBinaryType(),
                        toSql(predicate.getChild(1)));
            }

            @Override
            public String visitConstant(ConstantOperator literal, Void context) {
                if (literal.getType().equals(Type.VARCHAR)) {
                    return "'" + literal + "'";
                }
                return literal.toString();
            }

            @Override
            public String visitVariableReference(ColumnRefOperator variable, Void context) {
                return String.format("`%d: %s`", variable.getId(), variable.getName());
            }
        }, null);
    }
}
