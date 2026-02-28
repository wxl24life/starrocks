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

package com.starrocks.connector.paimon;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lakeoptimizer.LakeOptimizerRefreshManager;
import com.starrocks.lakeoptimizer.cache.LakeOptimizerCacheManager;
import com.starrocks.lakeoptimizer.cache.TableCacheKey;
import com.starrocks.lakeoptimizer.cache.TableSchemaInfo;
import com.starrocks.lakeoptimizer.query.LakeOptimizerQueryService;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachingPaimonCatalogTest {

    @Mocked
    private Catalog paimonNativeCatalog;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private LakeOptimizerCacheManager cacheManager;

    @Mocked
    private LakeOptimizerRefreshManager refreshManager;

    @Mocked
    private LakeOptimizerQueryService queryService;

    private CachingPaimonCatalog cachingCatalog;

    @Before
    public void setUp() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new Expectations() {{
                globalStateMgr.getLakeOptimizerCacheManager();
                result = cacheManager;
                minTimes = 0;

                globalStateMgr.getLakeOptimizerRefreshManager();
                result = refreshManager;
                minTimes = 0;

                globalStateMgr.getNextId();
                result = 999L;
                minTimes = 0;

                cacheManager.getQueryService();
                result = queryService;
                minTimes = 0;
            }};

        cachingCatalog = new CachingPaimonCatalog("test_catalog", paimonNativeCatalog);
    }

    @Test
    public void testGetystemTable(@Mocked FileStoreTable mockTable)
            throws Catalog.TableNotExistException {
        new Expectations() {{
                paimonNativeCatalog.getTable((Identifier) any);
                result = mockTable;

                mockTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, "f", new IntType())));

                mockTable.partitionKeys();
                result = Collections.emptyList();
            }};

        // System table should use cache with loader
        PaimonTable result = (PaimonTable) cachingCatalog.getTable("db", "tbl$manifests");
        Assert.assertEquals(PaimonTable.LakeOptimizerMode.DISABLED, result.getLakeOptimizerMode());
    }

    @Test
    public void testLoadFormatTable(@Mocked FormatTable mockTable)
            throws Catalog.TableNotExistException {
        List<Column> cols = Collections.singletonList(new Column("c", Type.INT, true));
        PaimonTable expected = new PaimonTable("test_catalog", "db", "fmt", cols, mockTable);
        expected.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.DISABLED);

        new Expectations() {{
                paimonNativeCatalog.getTable((Identifier) any);
                result = mockTable;

                mockTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, "c", new IntType())));
            }};
        TableCacheKey key = new TableCacheKey("test_catalog", "db", "fmt");
        PaimonTable result = Deencapsulation.invoke(cachingCatalog, "loadTable", key);
        Assert.assertEquals(PaimonTable.LakeOptimizerMode.DISABLED, result.getLakeOptimizerMode());
    }

    @Test
    public void testLoadTable(@Mocked FileStoreTable mockTable) throws Catalog.TableNotExistException {
        TableSchemaInfo schema = new TableSchemaInfo(100L, "test_catalog", "db", "tbl", "test-uuid", 1L, 5L, 2);

        new Expectations() {{
                paimonNativeCatalog.getTable((Identifier) any);
                result = mockTable;

                mockTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));

                mockTable.partitionKeys();
                result = Collections.emptyList();

                queryService.queryTableSchemaByName("test_catalog", "db", "tbl");
                result = schema;
            }};

        TableCacheKey key = new TableCacheKey("test_catalog", "db", "tbl");
        PaimonTable result = Deencapsulation.invoke(cachingCatalog, "loadTable", key);
        Assert.assertEquals(PaimonTable.LakeOptimizerMode.READY, result.getLakeOptimizerMode());
        Assert.assertEquals(schema.getTableId(), result.getId());
    }

    @Test
    public void tesLoadTableTriggersRefresh(@Mocked FileStoreTable mockTable)
            throws Catalog.TableNotExistException {
        List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
        PaimonTable uninit = new PaimonTable(999L, "test_catalog", "db", "uninit", cols, -1L, -1L, mockTable);
        uninit.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.UNINITIALIZED);
        AtomicBoolean refreshTriggered = new AtomicBoolean(false);

        new Expectations() {{
                paimonNativeCatalog.getTable((Identifier) any);
                result = mockTable;

                mockTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));

                mockTable.partitionKeys();
                result = Collections.emptyList();

                queryService.queryTableSchemaByName("test_catalog", "db", "uninit");
                result = null;

                refreshManager.triggerAsyncRefresh((PaimonTable) any);
                times = 1;
            }};


        TableCacheKey key = new TableCacheKey("test_catalog", "db", "uninit");
        PaimonTable result = Deencapsulation.invoke(cachingCatalog, "loadTable", key);
        Assert.assertEquals(PaimonTable.LakeOptimizerMode.UNINITIALIZED, result.getLakeOptimizerMode());

    }

    @Test
    public void testGetPartitions(@Mocked FileStoreTable mockTable) {
        List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
        PaimonTable ready = new PaimonTable(100L, "test_catalog", "db", "tbl", cols, 1L, 5L, mockTable);
        ready.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.READY);

        Partition p1 = new Partition("dt=2024-01-01", System.currentTimeMillis(), 1000L, 1024L, 10L);
        Partition p2 = new Partition("dt=2024-01-02", System.currentTimeMillis(), 2000L, 2048L, 20L);

        new Expectations() {{
                cacheManager.getTable((TableCacheKey) any, withNotNull());
                result = ready;

                cacheManager.getPartitions(100L, 5L, "tbl");
                result = Lists.newArrayList(p1, p2);
            }};

        Map<String, Partition> result = cachingCatalog.getPartitions("db", "tbl");
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("dt=2024-01-01"));
    }

    @Test
    public void testGetPartitionsFallback(@Mocked FileStoreTable mockTable) {
        List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
        PaimonTable uninit = new PaimonTable(999L, "test_catalog", "db", "uninit", cols, -1L, -1L, mockTable);
        uninit.setLakeOptimizerMode(PaimonTable.LakeOptimizerMode.UNINITIALIZED);

        new Expectations() {{
                cacheManager.getTable((TableCacheKey) any, withNotNull());
                result = uninit;

                mockTable.partitionKeys();
                result = Collections.emptyList();

                mockTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, "id", new IntType())));
            }};

        Map<String, Partition> result = cachingCatalog.getPartitions("db", "uninit");
        Assert.assertNotNull(result);
    }
}
