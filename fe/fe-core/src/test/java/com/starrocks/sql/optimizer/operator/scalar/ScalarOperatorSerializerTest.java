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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarOperatorSerializerTest {

    private static ColumnRefOperator vectorColumn() {
        return new ColumnRefOperator(1, new ArrayType(Type.FLOAT), "vector", true);
    }

    private static ArrayOperator queryArray() {
        // ARRAY<FLOAT>[0.1f, 0.2f, 0.3f] — use raw constructor to avoid checked SemanticException.
        List<ScalarOperator> children = ImmutableList.of(
                new ConstantOperator(0.1f, Type.FLOAT),
                new ConstantOperator(0.2f, Type.FLOAT),
                new ConstantOperator(0.3f, Type.FLOAT)
        );
        return new ArrayOperator(new ArrayType(Type.FLOAT), true, children);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getJsonArgs(Object json) {
        assertTrue(json instanceof Map, "top-level JSON must be Map");
        Map<String, Object> m = (Map<String, Object>) json;
        Object args = m.get(ScalarOperatorSerializer.ARGUMENTS);
        assertNotNull(args, "ARGUMENTS missing");
        assertTrue(args instanceof List, "ARGUMENTS must be List");
        return (List<Map<String, Object>>) args;
    }

    /**
     * approx_cosine_similarity([0.1,0.2,0.3], vector) — constant first, column second.
     * Serializer must swap so JSON args[0] is the column ref.
     */
    @Test
    public void testAnnArgsSwappedWhenConstantFirst() {
        CallOperator call = new CallOperator(
                FunctionSet.APPROX_COSINE_SIMILARITY,
                Type.FLOAT,
                ImmutableList.of(queryArray(), vectorColumn()));

        List<Map<String, Object>> args = getJsonArgs(ScalarOperatorSerializer.toJson(call));
        assertEquals(2, args.size());
        assertEquals("cr", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE),
                "ANN args[0] must be ColumnRefOperator after normalization");
        assertEquals("vector", args.get(0).get(ScalarOperatorSerializer.NAME));
        assertEquals("a", args.get(1).get(ScalarOperatorSerializer.OPERATOR_TYPE),
                "ANN args[1] must be the query ArrayOperator");
    }

    /**
     * approx_cosine_similarity(vector, [0.1,0.2,0.3]) — already column-first.
     * Serializer must leave order unchanged.
     */
    @Test
    public void testAnnArgsUnchangedWhenColumnFirst() {
        CallOperator call = new CallOperator(
                FunctionSet.APPROX_COSINE_SIMILARITY,
                Type.FLOAT,
                ImmutableList.of(vectorColumn(), queryArray()));

        List<Map<String, Object>> args = getJsonArgs(ScalarOperatorSerializer.toJson(call));
        assertEquals(2, args.size());
        assertEquals("cr", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("vector", args.get(0).get(ScalarOperatorSerializer.NAME));
        assertEquals("a", args.get(1).get(ScalarOperatorSerializer.OPERATOR_TYPE));
    }

    /**
     * Same normalization applies to the other two ANN functions.
     */
    @Test
    public void testAnnArgsSwappedForInnerProductAndL2() {
        for (String fn : List.of(FunctionSet.APPROX_INNER_PRODUCT, FunctionSet.APPROX_L2_DISTANCE)) {
            CallOperator call = new CallOperator(fn, Type.FLOAT,
                    ImmutableList.of(queryArray(), vectorColumn()));
            List<Map<String, Object>> args = getJsonArgs(ScalarOperatorSerializer.toJson(call));
            assertEquals("cr", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE),
                    fn + ": args[0] must be column ref after normalization");
        }
    }

    /**
     * Non-ANN calls keep their argument order regardless of types.
     * `<some_fn>([0.1,0.2,0.3], vector)` is not symmetric semantics here — we must NOT swap.
     */
    @Test
    public void testNonAnnArgsNotSwapped() {
        CallOperator call = new CallOperator("some_non_ann_fn", Type.FLOAT,
                ImmutableList.of(queryArray(), vectorColumn()));
        List<Map<String, Object>> args = getJsonArgs(ScalarOperatorSerializer.toJson(call));
        assertEquals("a", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE),
                "non-ANN args[0] must keep original ArrayOperator");
        assertEquals("cr", args.get(1).get(ScalarOperatorSerializer.OPERATOR_TYPE));
    }

    /**
     * ANN call where neither arg is a column ref (e.g. both constants) — leave unchanged,
     * BE will surface a clean InternalError.
     */
    @Test
    public void testAnnArgsBothNonColumnUnchanged() {
        CallOperator call = new CallOperator(FunctionSet.APPROX_COSINE_SIMILARITY, Type.FLOAT,
                ImmutableList.of(queryArray(), queryArray()));
        List<Map<String, Object>> args = getJsonArgs(ScalarOperatorSerializer.toJson(call));
        assertEquals(2, args.size());
        assertEquals("a", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("a", args.get(1).get(ScalarOperatorSerializer.OPERATOR_TYPE));
    }
}
