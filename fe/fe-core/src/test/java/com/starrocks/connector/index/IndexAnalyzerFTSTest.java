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

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.MatchType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexAnalyzerFTSTest {

    @Mocked
    ConnectorMetadata metadata;

    private static boolean savedRunningUnitTest;

    @BeforeAll
    public static void beforeAll() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterAll
    public static void afterAll() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    private PaimonTable buildPaimonTable(
            @Mocked FileStoreTable nativeTable,
            Map<String, String> tableOptions) {
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "content", SerializationUtils.newStringType(true)),
                new DataField(1, "id", new IntType(false))
        ));

        new Expectations() {
            {
                nativeTable.options();
                result = tableOptions;
                minTimes = 0;
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                minTimes = 0;
                nativeTable.rowType();
                result = rowType;
                minTimes = 0;
                nativeTable.primaryKeys();
                result = Collections.emptyList();
                minTimes = 0;
            }
        };

        List<Column> schema = Lists.newArrayList(
                new Column("content", ScalarType.createDefaultCatalogString()),
                new Column("id", ScalarType.INT)
        );
        return new PaimonTable("test_catalog", "test_db", "test_tbl", schema, nativeTable);
    }

    // ==================== FTS with lucene-fts index ====================

    @Test
    public void testMatchAllWithLuceneFtsIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello world");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertNotNull(prefilter);
        assertEquals(match, prefilter);
    }

    @Test
    public void testMatchAnyWithLuceneIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello world");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ANY, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertTrue(analyzer.canUsePredicateIndex());
    }

    @Test
    public void testMatchPhrasePrefilter(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("test document");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_PHRASE, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertTrue(analyzer.canUsePredicateIndex());
    }

    // ==================== FTS with match_prefix / match_wildcard (CallOperator) ====================

    @Test
    public void testMatchPrefixCallPrefilter(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hel");
        CallOperator call = new CallOperator("match_prefix", Type.BOOLEAN, List.of(col, text));

        IndexAnalyzer analyzer = new IndexAnalyzer(table, call, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertNotNull(prefilter);
        assertEquals(call, prefilter);
    }

    @Test
    public void testMatchWildcardCallPrefilter(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hel*o");
        CallOperator call = new CallOperator("match_wildcard", Type.BOOLEAN, List.of(col, text));

        IndexAnalyzer analyzer = new IndexAnalyzer(table, call, metadata);
        assertTrue(analyzer.canUsePredicateIndex());
    }

    // ==================== FTS with tantivy-fts index ====================

    @Test
    public void testMatchAllWithTantivyFtsIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.tantivy-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello world");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertNotNull(prefilter);
        assertEquals(match, prefilter);
    }

    @Test
    public void testMatchPrefixWithTantivyFtsIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.tantivy-fts.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hel");
        CallOperator call = new CallOperator("match_prefix", Type.BOOLEAN, List.of(col, text));

        IndexAnalyzer analyzer = new IndexAnalyzer(table, call, metadata);
        assertTrue(analyzer.canUsePredicateIndex());
    }

    // ==================== FTS with tantivy-fulltext index ====================

    @Test
    public void testMatchAllWithTantivyFulltextIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.tantivy-fulltext.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello world");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertNotNull(prefilter);
        assertEquals(match, prefilter);
    }

    @Test
    public void testMatchPrefixWithTantivyFulltextIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.tantivy-fulltext.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hel");
        CallOperator call = new CallOperator("match_prefix", Type.BOOLEAN, List.of(col, text));

        IndexAnalyzer analyzer = new IndexAnalyzer(table, call, metadata);
        assertTrue(analyzer.canUsePredicateIndex());
    }

    // ==================== Negative cases ====================

    @Test
    public void testFtsPredicateNoIndex(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable, Map.of());

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertFalse(analyzer.canUsePredicateIndex());
    }

    @Test
    public void testFtsPredicateWrongIndexType(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.bitmap.columns", "content"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertFalse(analyzer.canUsePredicateIndex());
    }

    @Test
    public void testFtsPredicateWrongColumn(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "other_col"));

        ColumnRefOperator col = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, col, text);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, match, metadata);
        assertFalse(analyzer.canUsePredicateIndex());
    }

    // ==================== Mixed predicates ====================

    @Test
    public void testMixedFtsAndBinaryPrefilter(@Mocked FileStoreTable nativeTable) {
        Map<String, String> options = new HashMap<>();
        options.put("global-index.lucene-fts.columns", "content");
        options.put("global-index.bitmap.columns", "id");
        PaimonTable table = buildPaimonTable(nativeTable, options);

        ColumnRefOperator contentCol = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, contentCol, text);

        ColumnRefOperator idCol = new ColumnRefOperator(2, Type.INT, "id", false);
        ConstantOperator idVal = ConstantOperator.createInt(42);
        BinaryPredicateOperator eq = new BinaryPredicateOperator(BinaryType.EQ, idCol, idVal);

        ScalarOperator combined = Utils.compoundAnd(match, eq);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, combined, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertNotNull(prefilter);

        ScalarOperator postfilter = analyzer.getPostfilter(false);
        assertNull(postfilter);
    }

    // ==================== Postfilter for non-indexed predicate ====================

    @Test
    public void testNonIndexedPredicateIsPostfilter(@Mocked FileStoreTable nativeTable) {
        PaimonTable table = buildPaimonTable(nativeTable,
                Map.of("global-index.lucene-fts.columns", "content"));

        ColumnRefOperator contentCol = new ColumnRefOperator(1, Type.VARCHAR, "content", true);
        ConstantOperator text = ConstantOperator.createVarchar("hello");
        MatchExprOperator match = new MatchExprOperator(MatchType.MATCH_ALL, contentCol, text);

        ColumnRefOperator idCol = new ColumnRefOperator(2, Type.INT, "id", false);
        ConstantOperator idVal = ConstantOperator.createInt(10);
        BinaryPredicateOperator gt = new BinaryPredicateOperator(BinaryType.GT, idCol, idVal);

        ScalarOperator combined = Utils.compoundAnd(match, gt);

        IndexAnalyzer analyzer = new IndexAnalyzer(table, combined, metadata);
        assertTrue(analyzer.canUsePredicateIndex());

        ScalarOperator prefilter = analyzer.getPrefilter();
        assertEquals(match, prefilter);

        ScalarOperator postfilter = analyzer.getPostfilter(false);
        assertNotNull(postfilter);
        assertEquals(gt, postfilter);
    }
}
