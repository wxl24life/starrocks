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

package com.starrocks.lakeoptimizer.query;

import com.google.common.collect.Lists;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TMetadataEntry;
import com.starrocks.thrift.TPaimonFileMetadata;
import com.starrocks.thrift.TPaimonMetadataType;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ExternalManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.thrift.TSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;

/**
 * Round-trip tests for the FE-side lake optimizer serde:
 *   Paimon (real write) -> ExternalManifestEntry (source of truth)
 *                       -> TPaimonFileMetadata (mirrors PaimonMetaWriter)
 *                       -> TResultBatch (simulated BE response)
 *                       -> LakeOptimizerQueryService.parseFileStatsResults
 *                       -> ExternalManifestEntry'
 *
 * The BE-side persistence (entity-table INSERT/SELECT) is intentionally skipped — these
 * tests exercise only the FE serde contract between PaimonMetaWriter and
 * LakeOptimizerQueryService.
 */
public class LakeOptimizerRoundTripTest {

    @Test
    public void testRoundTripUnpartitionedAppendOnly() throws Exception {
        Catalog catalog = newCatalog();
        catalog.createDatabase("db", true);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
        Identifier id = Identifier.create("db", "t_append");
        catalog.createTable(id, schema, true);

        Table table = catalog.getTable(id);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        BatchTableWrite w = wb.newWrite();
        w.write(GenericRow.of(1, BinaryString.fromString("a")));
        w.write(GenericRow.of(2, BinaryString.fromString("b")));
        BatchTableCommit commit = wb.newCommit();
        commit.commit(w.prepareCommit());

        runRoundTrip((FileStoreTable) table, /*isUnpartitioned*/ true);
    }

    @Test
    public void testRoundTripPkTableWithStats() throws Exception {
        // PK table populates minKey/maxKey on each file and produces non-empty keyStats —
        // exercises the BinaryRow / SimpleStats serde paths that the append-only test skips.
        Catalog catalog = newCatalog();
        catalog.createDatabase("db", true);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "2")
                .build();
        Identifier id = Identifier.create("db", "t_pk");
        catalog.createTable(id, schema, true);

        Table table = catalog.getTable(id);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        BatchTableWrite w = wb.newWrite();
        w.write(GenericRow.of(10, BinaryString.fromString("a")));
        w.write(GenericRow.of(20, BinaryString.fromString("b")));
        w.write(GenericRow.of(30, BinaryString.fromString("c")));
        BatchTableCommit commit = wb.newCommit();
        commit.commit(w.prepareCommit());

        FileStoreTable fst = (FileStoreTable) table;
        List<ExternalManifestEntry> source = runRoundTrip(fst, /*isUnpartitioned*/ true);

        // Sanity: at least one file should carry non-empty key stats — otherwise this test
        // degenerates to the append-only case and the new coverage is illusory.
        boolean anyMinKey = source.stream().anyMatch(e -> {
            BinaryRow mk = e.manifestEntry().file().minKey();
            return mk != null && mk.getFieldCount() > 0;
        });
        Assert.assertTrue("PK table should produce files with non-empty minKey", anyMinKey);
    }

    @Test
    public void testRoundTripPartitionedTable() throws Exception {
        // Partitioned table exercises partitionMap lookup with isUnpartitioned=false.
        Catalog catalog = newCatalog();
        catalog.createDatabase("db", true);
        Schema schema = Schema.newBuilder()
                .column("dt", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .partitionKeys("dt")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.BUCKET_KEY.key(), "id")
                .build();
        Identifier id = Identifier.create("db", "t_part");
        catalog.createTable(id, schema, true);

        Table table = catalog.getTable(id);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        BatchTableWrite w = wb.newWrite();
        w.write(GenericRow.of(BinaryString.fromString("2024-01-01"), 1));
        w.write(GenericRow.of(BinaryString.fromString("2024-01-02"), 2));
        BatchTableCommit commit = wb.newCommit();
        commit.commit(w.prepareCommit());

        FileStoreTable fst = (FileStoreTable) table;
        Long snapshotId = fst.snapshotManager().latestSnapshotId();
        Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> grouped =
                fst.listExternalManifestEntries(snapshotId);

        // Build BinaryRow -> partition_name (using same Paimon utilities as PaimonMetaWriter)
        // and Map<String, Partition> for the parser.
        Map<BinaryRow, String> nameByPartition = new HashMap<>();
        Map<String, Partition> partitionMap = new HashMap<>();
        InternalRowPartitionComputer computer = newPartitionComputer(fst);
        for (BinaryRow part : grouped.keySet()) {
            String name = PartitionPathUtils.generatePartitionPath(computer.generatePartValues(part));
            nameByPartition.put(part, name);
            Partition p = new Partition(name, 0L, 0L, 0L, 0L);
            p.setPartitionValue(part);
            partitionMap.put(name, p);
        }
        Assert.assertEquals(2, partitionMap.size());

        List<ExternalManifestEntry> source = flatten(grouped);
        // Map each source entry to its partition_name via its BinaryRow partition key.
        Map<ExternalManifestEntry, String> nameByEntry = new HashMap<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> pe : grouped.entrySet()) {
            String name = nameByPartition.get(pe.getKey());
            for (List<ExternalManifestEntry> bucketEntries : pe.getValue().values()) {
                for (ExternalManifestEntry eme : bucketEntries) {
                    nameByEntry.put(eme, name);
                }
            }
        }

        TResultBatch batch = buildResultBatch(source, nameByEntry::get);
        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                return Collections.singletonList(batch);
            }
        };

        LakeOptimizerQueryService svc = new LakeOptimizerQueryService();
        List<ExternalManifestEntry> roundTripped = svc.queryManifestAll(
                /*tableId*/ 200L, snapshotId, partitionMap, /*isUnpartitioned*/ false);

        Assert.assertEquals(source.size(), roundTripped.size());
        sortByName(source);
        sortByName(roundTripped);
        for (int i = 0; i < source.size(); i++) {
            assertEntryEquivalent(source.get(i), roundTripped.get(i));
        }
        // Each round-tripped entry's BinaryRow partition key should match one of the source partitions.
        for (ExternalManifestEntry rt : roundTripped) {
            byte[] partBytes = SerializationUtils.serializeBinaryRow(rt.manifestEntry().partition());
            boolean matched = nameByPartition.keySet().stream()
                    .anyMatch(p -> java.util.Arrays.equals(SerializationUtils.serializeBinaryRow(p), partBytes));
            Assert.assertTrue("round-tripped partition should match a source partition", matched);
        }
    }

    @Test
    public void testRoundTripWithDeletionFile() throws Exception {
        // Take an entry from a simple write and synthetically wrap it with a DeletionFile to
        // exercise the four deletion_* fields without standing up Paimon's DV machinery.
        Catalog catalog = newCatalog();
        catalog.createDatabase("db", true);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .build();
        Identifier id = Identifier.create("db", "t_dv");
        catalog.createTable(id, schema, true);
        Table table = catalog.getTable(id);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        BatchTableWrite w = wb.newWrite();
        w.write(GenericRow.of(42));
        BatchTableCommit commit = wb.newCommit();
        commit.commit(w.prepareCommit());

        FileStoreTable fst = (FileStoreTable) table;
        Long snapshotId = fst.snapshotManager().latestSnapshotId();
        List<ExternalManifestEntry> raw = flatten(fst.listExternalManifestEntries(snapshotId));
        Assert.assertFalse(raw.isEmpty());

        DeletionFile dv = new DeletionFile("/tmp/dv-test.bin", 128L, 64L, 7L);
        ExternalManifestEntry sourceEntry = new ExternalManifestEntry(raw.get(0).manifestEntry(), dv);
        List<ExternalManifestEntry> source = Collections.singletonList(sourceEntry);

        TResultBatch batch = buildResultBatch(source, e -> "");
        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                return Collections.singletonList(batch);
            }
        };

        LakeOptimizerQueryService svc = new LakeOptimizerQueryService();
        List<ExternalManifestEntry> roundTripped = svc.queryManifestAll(
                300L, snapshotId, Collections.<String, Partition>emptyMap(), true);

        Assert.assertEquals(1, roundTripped.size());
        DeletionFile actual = roundTripped.get(0).deletionFile();
        Assert.assertNotNull("DeletionFile should round-trip", actual);
        Assert.assertEquals(dv.path(), actual.path());
        Assert.assertEquals(dv.offset(), actual.offset());
        Assert.assertEquals(dv.length(), actual.length());
        Assert.assertEquals(dv.cardinality(), actual.cardinality());
        // The wrapped manifest entry should still survive the round-trip intact.
        assertEntryEquivalent(sourceEntry, roundTripped.get(0));
    }

    // ==================== Shared helpers ====================

    private static Catalog newCatalog() throws Exception {
        java.nio.file.Path tmpDir = Files.createTempDirectory("lo_rr_");
        return CatalogFactory.createCatalog(
                CatalogContext.create(new org.apache.paimon.fs.Path(tmpDir.toString())));
    }

    /**
     * Run the standard "read source -> serialize -> mock -> parse -> assert" round-trip for
     * an unpartitioned table. Returns the source entries so the caller can add extra
     * scenario-specific assertions.
     */
    private static List<ExternalManifestEntry> runRoundTrip(FileStoreTable fst, boolean isUnpartitioned) throws Exception {
        Long snapshotId = fst.snapshotManager().latestSnapshotId();
        Assert.assertNotNull(snapshotId);

        List<ExternalManifestEntry> source = flatten(fst.listExternalManifestEntries(snapshotId));
        Assert.assertFalse("Paimon should produce at least one manifest entry", source.isEmpty());

        TResultBatch batch = buildResultBatch(source, e -> "");
        new MockUp<LakeOptimizerQueryService>() {
            @Mock
            public List<TResultBatch> executeQuery(String sql, TPaimonMetadataType type) {
                return Collections.singletonList(batch);
            }
        };

        LakeOptimizerQueryService svc = new LakeOptimizerQueryService();
        List<ExternalManifestEntry> roundTripped = svc.queryManifestAll(
                100L, snapshotId, Collections.<String, Partition>emptyMap(), isUnpartitioned);

        Assert.assertEquals(source.size(), roundTripped.size());
        sortByName(source);
        sortByName(roundTripped);
        for (int i = 0; i < source.size(); i++) {
            assertEntryEquivalent(source.get(i), roundTripped.get(i));
        }
        return source;
    }

    private static List<ExternalManifestEntry> flatten(
            Map<BinaryRow, Map<Integer, List<ExternalManifestEntry>>> grouped) {
        return grouped.values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private static void sortByName(List<ExternalManifestEntry> entries) {
        entries.sort(Comparator.comparing(e -> e.manifestEntry().file().fileName()));
    }

    private static InternalRowPartitionComputer newPartitionComputer(FileStoreTable fst) {
        Options options = Options.fromMap(fst.options());
        return new InternalRowPartitionComputer(
                options.get(PARTITION_DEFAULT_NAME),
                fst.rowType().project(fst.partitionKeys()),
                fst.partitionKeys().toArray(new String[0]),
                false);
    }

    private static TResultBatch buildResultBatch(List<ExternalManifestEntry> entries,
                                                 Function<ExternalManifestEntry, String> partitionNameFn) throws Exception {
        TResultBatch batch = new TResultBatch();
        batch.setIs_compressed(false);
        batch.setPacket_seq(0);
        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer();
        List<ByteBuffer> rows = Lists.newArrayList();
        for (ExternalManifestEntry eme : entries) {
            TMetadataEntry entry = new TMetadataEntry();
            entry.setPaimon_file_metadata(toThrift(eme, partitionNameFn.apply(eme)));
            rows.add(ByteBuffer.wrap(serializer.serialize(entry)));
        }
        batch.setRows(rows);
        return batch;
    }

    /**
     * Mirrors the field mapping that {@link com.starrocks.lakeoptimizer.PaimonMetaWriter}
     * establishes when serializing an ExternalManifestEntry into the entity-table row.
     * Any drift between this method and PaimonMetaWriter or
     * LakeOptimizerQueryService.parseExternalManifestEntryFromThrift will fail the
     * round-trip assertion.
     */
    private static TPaimonFileMetadata toThrift(ExternalManifestEntry eme, String partitionName) {
        ManifestEntry me = eme.manifestEntry();
        DataFileMeta f = me.file();
        TPaimonFileMetadata t = new TPaimonFileMetadata();
        t.setPartition_name(partitionName);
        t.setBucket(me.bucket());
        t.setTotal_buckets(me.totalBuckets());
        t.setFile_kind(me.kind().toByteValue());
        t.setFile_name(f.fileName());
        t.setFile_size(f.fileSize());
        t.setRow_count(f.rowCount());
        t.setSchema_id(f.schemaId());
        t.setLevel(f.level());
        t.setMin_sequence_number(f.minSequenceNumber());
        t.setMax_sequence_number(f.maxSequenceNumber());
        t.setCreation_time(f.creationTimeEpochMillis());

        if (f.minKey() != null) {
            t.setMin_key(SerializationUtils.serializeBinaryRow(f.minKey()));
        }
        if (f.maxKey() != null) {
            t.setMax_key(SerializationUtils.serializeBinaryRow(f.maxKey()));
        }

        SimpleStats ks = f.keyStats();
        if (ks.minValues() != null) {
            t.setKey_stats_min(SerializationUtils.serializeBinaryRow(ks.minValues()));
        }
        if (ks.maxValues() != null) {
            t.setKey_stats_max(SerializationUtils.serializeBinaryRow(ks.maxValues()));
        }
        if (ks.nullCounts() != null) {
            t.setKey_stats_null_count(ks.nullCounts().toBytes());
        }

        SimpleStats vs = f.valueStats();
        if (vs.minValues() != null) {
            t.setValue_stats_min(SerializationUtils.serializeBinaryRow(vs.minValues()));
        }
        if (vs.maxValues() != null) {
            t.setValue_stats_max(SerializationUtils.serializeBinaryRow(vs.maxValues()));
        }
        if (vs.nullCounts() != null) {
            t.setValue_stats_null_count(vs.nullCounts().toBytes());
        }

        if (f.extraFiles() != null) {
            t.setExtra_files(f.extraFiles());
        }
        f.deleteRowCount().ifPresent(t::setDelete_row_count);
        if (f.embeddedIndex() != null && f.embeddedIndex().length > 0) {
            t.setEmbedded_file_index(f.embeddedIndex());
        }
        f.fileSource().ifPresent(fs -> t.setFile_source((byte) fs.ordinal()));
        if (f.valueStatsCols() != null && !f.valueStatsCols().isEmpty()) {
            t.setValue_stats_cols(f.valueStatsCols());
        }
        f.externalPath().ifPresent(t::setExternal_path);
        if (f.firstRowId() != null) {
            t.setFirst_row_id(f.firstRowId());
        }
        if (f.writeCols() != null && !f.writeCols().isEmpty()) {
            t.setWrite_cols(f.writeCols());
        }

        DeletionFile df = eme.deletionFile();
        if (df != null) {
            t.setDeletion_path(df.path());
            t.setDeletion_offset(df.offset());
            t.setDeletion_length(df.length());
            t.setDeletion_cardinality(df.cardinality());
        }
        return t;
    }

    private static void assertEntryEquivalent(ExternalManifestEntry expected, ExternalManifestEntry actual) {
        ManifestEntry e = expected.manifestEntry();
        ManifestEntry a = actual.manifestEntry();
        Assert.assertEquals(e.kind(), a.kind());
        Assert.assertEquals(e.bucket(), a.bucket());
        Assert.assertEquals(e.totalBuckets(), a.totalBuckets());

        DataFileMeta ef = e.file();
        DataFileMeta af = a.file();
        Assert.assertEquals(ef.fileName(), af.fileName());
        Assert.assertEquals(ef.fileSize(), af.fileSize());
        Assert.assertEquals(ef.rowCount(), af.rowCount());
        Assert.assertEquals(ef.schemaId(), af.schemaId());
        Assert.assertEquals(ef.level(), af.level());
        Assert.assertEquals(ef.minSequenceNumber(), af.minSequenceNumber());
        Assert.assertEquals(ef.maxSequenceNumber(), af.maxSequenceNumber());
        Assert.assertEquals(ef.creationTimeEpochMillis(), af.creationTimeEpochMillis());

        // Compare BinaryRow / SimpleStats via the same serialization path used by the
        // round-trip — null and EMPTY_ROW are treated as equivalent because the writer
        // emits null for an absent row but the reader returns EMPTY_ROW.
        Assert.assertArrayEquals(serBytes(ef.minKey()), serBytes(af.minKey()));
        Assert.assertArrayEquals(serBytes(ef.maxKey()), serBytes(af.maxKey()));
        Assert.assertArrayEquals(serBytes(ef.keyStats().minValues()), serBytes(af.keyStats().minValues()));
        Assert.assertArrayEquals(serBytes(ef.keyStats().maxValues()), serBytes(af.keyStats().maxValues()));
        Assert.assertArrayEquals(serBytes(ef.valueStats().minValues()), serBytes(af.valueStats().minValues()));
        Assert.assertArrayEquals(serBytes(ef.valueStats().maxValues()), serBytes(af.valueStats().maxValues()));
        Assert.assertArrayEquals(ef.valueStats().nullCounts().toBytes(), af.valueStats().nullCounts().toBytes());
    }

    private static byte[] serBytes(BinaryRow r) {
        BinaryRow row = (r == null) ? BinaryRow.EMPTY_ROW : r;
        return SerializationUtils.serializeBinaryRow(row);
    }
}
