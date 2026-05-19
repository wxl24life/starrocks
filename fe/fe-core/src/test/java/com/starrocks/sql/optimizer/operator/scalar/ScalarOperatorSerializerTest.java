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

import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.MatchType;
import com.starrocks.catalog.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ScalarOperatorSerializerTest {

    private final ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
    private final ConstantOperator text = ConstantOperator.createVarchar("hello world");

    // ==================== toJson — MatchExprOperator ====================

    @Test
    public void testMatchAllToJson() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);
        Map<String, Object> json = (Map<String, Object>) ScalarOperatorSerializer.toJson(match);

        assertEquals("ca", json.get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("match_all", json.get(ScalarOperatorSerializer.FN_NAME));

        List<Map<String, Object>> args = (List<Map<String, Object>>) json.get(ScalarOperatorSerializer.ARGUMENTS);
        assertEquals(2, args.size());
        assertEquals("cr", args.get(0).get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("content", args.get(0).get(ScalarOperatorSerializer.NAME));
        assertEquals("co", args.get(1).get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("hello world", args.get(1).get(ScalarOperatorSerializer.VALUE));
    }

    @Test
    public void testMatchAnyToJson() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ANY, col, text);
        Map<String, Object> json = (Map<String, Object>) ScalarOperatorSerializer.toJson(match);
        assertEquals("match_any", json.get(ScalarOperatorSerializer.FN_NAME));
    }

    @Test
    public void testMatchPhraseToJson() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_PHRASE, col, text);
        Map<String, Object> json = (Map<String, Object>) ScalarOperatorSerializer.toJson(match);
        assertEquals("match_phrase", json.get(ScalarOperatorSerializer.FN_NAME));
    }

    @Test
    public void testCallOperatorToJson() {
        CallOperator call = new CallOperator("match_prefix", Type.BOOLEAN, List.of(col, text));
        Map<String, Object> json = (Map<String, Object>) ScalarOperatorSerializer.toJson(call);

        assertEquals("ca", json.get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("match_prefix", json.get(ScalarOperatorSerializer.FN_NAME));

        List<Map<String, Object>> args = (List<Map<String, Object>>) json.get(ScalarOperatorSerializer.ARGUMENTS);
        assertEquals(2, args.size());
    }

    // ==================== toJson — BinaryPredicate ====================

    @Test
    public void testBinaryPredicateToJson() {
        ColumnRefOperator id = new ColumnRefOperator(2, Type.INT, "id", false);
        ConstantOperator val = ConstantOperator.createInt(42);
        BinaryPredicateOperator pred = new BinaryPredicateOperator(BinaryType.EQ, id, val);

        Map<String, Object> json = (Map<String, Object>) ScalarOperatorSerializer.toJson(pred);
        assertEquals("b", json.get(ScalarOperatorSerializer.OPERATOR_TYPE));
        assertEquals("EQ", json.get(ScalarOperatorSerializer.BINARY_TYPE));
    }

    // ==================== toSql — MatchExprOperator ====================

    @Test
    public void testMatchAllToSql() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);
        String sql = ScalarOperatorSerializer.toSql(match);
        assertEquals("`1: content` MATCH_ALL 'hello world'", sql);
    }

    @Test
    public void testMatchAnyToSql() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ANY, col, text);
        String sql = ScalarOperatorSerializer.toSql(match);
        assertEquals("`1: content` MATCH_ANY 'hello world'", sql);
    }

    @Test
    public void testMatchPhraseToSql() {
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_PHRASE, col, text);
        String sql = ScalarOperatorSerializer.toSql(match);
        assertEquals("`1: content` MATCH_PHRASE 'hello world'", sql);
    }

    @Test
    public void testCallOperatorToSql() {
        CallOperator call = new CallOperator("match_prefix", Type.BOOLEAN, List.of(col, text));
        String sql = ScalarOperatorSerializer.toSql(call);
        assertEquals("match_prefix(`1: content`, 'hello world')", sql);
    }
}
