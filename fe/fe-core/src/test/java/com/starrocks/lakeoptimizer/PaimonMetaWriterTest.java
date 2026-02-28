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

package com.starrocks.lakeoptimizer;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntryWithDeletionFile;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonMetaWriterTest {

    @Mocked
    private ConnectContext context;

    @Mocked
    private TableSchema schema;

    @Mocked
    private Table nativeTable;

    @Test
    public void testWriteTableMetadataBatched() throws Exception {
        int oldBatchSize = Config.lake_optimizer_refresh_partition_batch_size;
        int oldRetention = Config.lake_optimizer_snapshot_retention_count;
        try {
            Config.lake_optimizer_refresh_partition_batch_size = 2;
            Config.lake_optimizer_snapshot_retention_count = 5;

            mockNativeTable("p");
            new Expectations() {{
                    schema.id();
                    result = 10L;
                    minTimes = 0;
                }};

            List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
            PaimonTable table = new PaimonTable(100L, "catalog", "db", "tbl", cols, 1L, 5L, nativeTable);
            Map<BinaryRow, Map<Integer, List<ManifestEntryWithDeletionFile>>> groupedManifests =
                    createGroupedManifests(3);

            AtomicInteger insertCount = new AtomicInteger(0);
            List<String> deleteSqls = new ArrayList<>();
            new MockUp<PaimonMetaWriter>() {
                @Mock
                public void executeInsert(ConnectContext ctx, String tableName,
                                          List<String> columnNames, List<List<Expr>> rows) {
                    insertCount.incrementAndGet();
                }

                @Mock
                public void executeDelete(ConnectContext ctx, String sql) {
                    deleteSqls.add(sql);
                }
            };

            PaimonMetaWriter writer = new PaimonMetaWriter("test_catalog");
            writer.writeTableMetadata(context, table, 10L, schema, "test-uuid", groupedManifests);
            // verify inserts
            int expectedBatches = (3 + 2 - 1) / 2;
            int expectedInserts = expectedBatches * 2 + 1;
            Assert.assertEquals(expectedInserts, insertCount.get());
            // verify clean up after successful inserts
            Assert.assertEquals(2, deleteSqls.size());
            Assert.assertTrue(deleteSqls.get(0).contains("table_id = 100"));
            Assert.assertTrue(deleteSqls.get(1).contains("table_id = 100"));
            Assert.assertTrue(deleteSqls.get(0).contains("snapshot_id < 5"));
            Assert.assertTrue(deleteSqls.get(1).contains("snapshot_id < 5"));
        } finally {
            Config.lake_optimizer_refresh_partition_batch_size = oldBatchSize;
            Config.lake_optimizer_snapshot_retention_count = oldRetention;
        }
    }

    @Test
    public void testCleanupExpiredSnapshotsSkipped() throws Exception {
        int oldRetention = Config.lake_optimizer_snapshot_retention_count;
        try {
            Config.lake_optimizer_snapshot_retention_count = 3;

            List<String> deleteSqls = new ArrayList<>();
            new MockUp<PaimonMetaWriter>() {
                @Mock
                public void executeDelete(ConnectContext ctx, String sql) {
                    deleteSqls.add(sql);
                }
            };
            PaimonTable table = createTable(100L, 9L, 10L);

            PaimonMetaWriter writer = new PaimonMetaWriter("test_catalog");
            Deencapsulation.invoke(writer, "cleanupExpiredSnapshots", context, table, 11L);

            Assert.assertTrue(deleteSqls.isEmpty());
        } finally {
            Config.lake_optimizer_snapshot_retention_count = oldRetention;
        }
    }

    @Test
    public void testWriteFailureTriggersCleanup() throws Exception {
        int oldBatchSize = Config.lake_optimizer_refresh_partition_batch_size;
        int oldRetention = Config.lake_optimizer_snapshot_retention_count;
        try {
            Config.lake_optimizer_refresh_partition_batch_size = 0;
            Config.lake_optimizer_snapshot_retention_count = 0;

            mockNativeTable("p");
            new Expectations() {{
                    schema.id();
                    result = 10L;
                    minTimes = 0;
                }};

            List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
            PaimonTable table = new PaimonTable(100L, "catalog", "db", "tbl", cols, 1L, 5L, nativeTable);
            Map<BinaryRow, Map<Integer, List<ManifestEntryWithDeletionFile>>> groupedManifests =
                    createGroupedManifests(1);

            List<String> deleteSqls = new ArrayList<>();
            new MockUp<PaimonMetaWriter>() {
                @Mock
                public void executeInsert(ConnectContext ctx, String tableName,
                                          List<String> columnNames, List<List<Expr>> rows) {
                    throw new RuntimeException("write failed");
                }

                @Mock
                public void executeDelete(ConnectContext ctx, String sql) {
                    deleteSqls.add(sql);
                }
            };

            PaimonMetaWriter writer = new PaimonMetaWriter("test_catalog");
            try {
                writer.writeTableMetadata(context, table, 12L, schema, "test-uuid", groupedManifests);
                Assert.fail("Expected write to fail");
            } catch (RuntimeException ignored) {
                // expected
            }

            Assert.assertEquals(2, deleteSqls.size());
            Assert.assertTrue(deleteSqls.get(0).contains("table_id = 100"));
            Assert.assertTrue(deleteSqls.get(1).contains("table_id = 100"));
            Assert.assertTrue(deleteSqls.get(0).contains("snapshot_id = 12"));
            Assert.assertTrue(deleteSqls.get(1).contains("snapshot_id = 12"));
        } finally {
            Config.lake_optimizer_refresh_partition_batch_size = oldBatchSize;
            Config.lake_optimizer_snapshot_retention_count = oldRetention;
        }
    }

    private void mockNativeTable(String partitionKey) {
        new Expectations() {{
                nativeTable.options();
                result = Collections.emptyMap();
                nativeTable.partitionKeys();
                result = Collections.singletonList(partitionKey);
                nativeTable.rowType();
                result = new RowType(Collections.singletonList(new DataField(0, partitionKey, new IntType())));
                minTimes = 0;
            }};
    }

    private PaimonTable createTable(long tableId, long beginSnapshot, long endSnapshot) {
        List<Column> cols = Collections.singletonList(new Column("id", Type.INT, true));
        return new PaimonTable(tableId, "catalog", "db", "tbl", cols, beginSnapshot, endSnapshot, nativeTable);
    }

    private Map<BinaryRow, Map<Integer, List<ManifestEntryWithDeletionFile>>> createGroupedManifests(
            int partitionCount) {
        Map<BinaryRow, Map<Integer, List<ManifestEntryWithDeletionFile>>> grouped = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            BinaryRow partition = createPartitionRow(i);
            DataFileMeta fileMeta = DataFileMeta.forAppend(
                    "file-" + i,
                    10L,
                    5L,
                    SimpleStats.EMPTY_STATS,
                    1L,
                    1L,
                    1L,
                    Collections.emptyList(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            ManifestEntry entry = ManifestEntry.create(FileKind.ADD, partition, 0, 1, fileMeta);
            ManifestEntryWithDeletionFile wrapper = new ManifestEntryWithDeletionFile(entry);
            Map<Integer, List<ManifestEntryWithDeletionFile>> bucketMap = new HashMap<>();
            bucketMap.put(0, Collections.singletonList(wrapper));
            grouped.put(partition, bucketMap);
        }
        return grouped;
    }

    private BinaryRow createPartitionRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    @Test
    public void testBinaryRowToLiteral() {
        // Multi-field BinaryRow (simulates a composite partition key or stats row)
        BinaryRow original = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(original);
        writer.writeInt(0, 100);
        writer.writeLong(1, 999999L);
        writer.writeInt(2, -1);
        writer.complete();

        byte[] serialized = SerializationUtils.serializeBinaryRow(original);
        BinaryRow deserialized = SerializationUtils.deserializeBinaryRow(serialized);

        Assert.assertEquals(original.getFieldCount(), deserialized.getFieldCount());
        Assert.assertEquals(original.getInt(0), deserialized.getInt(0));
        Assert.assertEquals(original.getLong(1), deserialized.getLong(1));
        Assert.assertEquals(original.getInt(2), deserialized.getInt(2));
    }

    @Test
    public void testBinaryArrayToLiteral() {
        // BinaryArray with null elements
        Long[] nullCountValues = new Long[] {1L, null, 3L};
        BinaryArray original = BinaryArray.fromLongArray(nullCountValues);

        byte[] serialized = original.toBytes();

        BinaryArray deserialized = new BinaryArray();
        deserialized.pointTo(MemorySegment.wrap(serialized), 0, serialized.length);

        Assert.assertEquals(original.size(), deserialized.size());
        Assert.assertEquals(1L, deserialized.getLong(0));
        Assert.assertTrue(deserialized.isNullAt(1));
        Assert.assertEquals(3L, deserialized.getLong(2));
    }
}
