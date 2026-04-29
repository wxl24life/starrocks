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

import com.starrocks.common.Pair;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class PaimonUtils {

    public static TIcebergSchema getTPaimonSchema(RowType rowType) {
        // reuse TIcebergSchema directly for compatibility.
        TIcebergSchema tPaimonSchema = new TIcebergSchema();
        List<DataField> paimonFields = rowType.getFields();
        List<TIcebergSchemaField> tIcebergFields = new ArrayList<>(paimonFields.size());
        for (DataField field : paimonFields) {
            tIcebergFields.add(getTPaimonSchemaField(field.name(), field.type(), field.id(), 0, -1));
        }
        tPaimonSchema.setFields(tIcebergFields);
        return tPaimonSchema;
    }

    public static TIcebergSchemaField getTPaimonSchemaField(String name, DataType type, int fieldId, int depth, int parentId) {
        TIcebergSchemaField tPaimonSchemaField = new TIcebergSchemaField();
        if (parentId != -1) {
            tPaimonSchemaField.setField_id(parentId);
        } else {
            tPaimonSchemaField.setField_id(fieldId);
        }
        tPaimonSchemaField.setName(name);
        if (type.getTypeRoot() == DataTypeRoot.MAP) {
            org.apache.paimon.types.MapType mapType = (MapType) type;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            int mapKeyFieldId = SpecialFields.getMapKeyFieldId(fieldId, depth + 1);
            int mapValueFieldId = SpecialFields.getMapValueFieldId(fieldId, depth + 1);
            List<TIcebergSchemaField> children = new ArrayList<>(2);
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.MAP_KEY_NAME, keyType, fieldId, depth + 1, mapKeyFieldId));
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.MAP_VALUE_NAME, valueType, fieldId,
                    depth + 1, mapValueFieldId));
            tPaimonSchemaField.setChildren(children);
        }
        if (type.getTypeRoot() == DataTypeRoot.ARRAY) {
            org.apache.paimon.types.ArrayType arrayType = (ArrayType) type;
            DataType elementType = arrayType.getElementType();
            int elementId = SpecialFields.getArrayElementFieldId(fieldId, depth + 1);
            List<TIcebergSchemaField> children = new ArrayList<>(1);
            children.add(getTPaimonSchemaField(ParquetSchemaConverter.LIST_ELEMENT_NAME, elementType, fieldId,
                    depth + 1, elementId));
            tPaimonSchemaField.setChildren(children);
        }
        // the parent id of row type is always -1, refer to: org.apache.paimon.format.parquet.ParquetSchemaConverter
        if (type.getTypeRoot() == DataTypeRoot.ROW) {
            RowType rowType = (RowType) type;
            List<DataField> childrenFields = rowType.getFields();
            List<TIcebergSchemaField> children = new ArrayList<>(rowType.getFieldCount());
            for (DataField childrenField : childrenFields) {
                children.add(getTPaimonSchemaField(childrenField.name(), childrenField.type(), childrenField.id(),
                        depth + 1, -1));
            }
            tPaimonSchemaField.setChildren(children);
        }
        return tPaimonSchemaField;
    }

    private static void addToTopNQueue(
            PriorityQueue<Pair<Long, Float>> topNQueue,
            long limitGlobal,
            List<Pair<Long, Float>> pairs
    ) {
        for (Pair<Long, Float> pair : pairs) {
            if (topNQueue.size() < limitGlobal) {
                topNQueue.add(pair);
            } else {
                if (pair.second > topNQueue.peek().second) {
                    topNQueue.poll();
                    topNQueue.add(pair);
                }
            }
        }
    }

    // Used when every shard returned a NULL row (no matches). Returning an empty result instead
    // of null lets DataEvolutionBatchScan produce zero-row splits naturally rather than the FE
    // raising a fatal exception.
    public static GlobalIndexResult createEmptyGlobalIndexResult(IndexCondition indexCondition) {
        if (indexCondition instanceof TopNIndexCondition) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        return GlobalIndexResult.createEmpty();
    }

    public static GlobalIndexResultAggregator createGlobalIndexResultAggregator(
            GlobalIndexResult globalIndexResult,
            IndexCondition indexCondition
    ) {
        if (globalIndexResult instanceof ScoredGlobalIndexResult) {
            if (!(indexCondition instanceof TopNIndexCondition)) {
                throw new RuntimeException("TopNIndexCondition is required for ScoredGlobalIndexResult");
            }
            long[] nLocal = ((TopNIndexCondition) indexCondition).getNLocal();
            long limitGlobal = Arrays.stream(nLocal).sum();
            return createAggregatorWithScorePriority((ScoredGlobalIndexResult) globalIndexResult, limitGlobal);
        } else {
            return createAggregator(globalIndexResult);
        }
    }

    @NotNull
    public static GlobalIndexResultAggregator createAggregator(GlobalIndexResult globalIndexResult) {
        return new GlobalIndexResultAggregator() {
            @Override
            public void iterate(GlobalIndexResult partial) {
                globalIndexResult.or(partial);
            }

            @Override
            public GlobalIndexResult terminate() {
                return globalIndexResult;
            }
        };
    }

    @NotNull
    public static GlobalIndexResultAggregator createAggregatorWithScorePriority(
            ScoredGlobalIndexResult globalIndexResult, long limitGlobal) {
        // PriorityQueue requires capacity >= 1; when every shard's localN is 0 (no candidates),
        // start with capacity 1 — the inner addToTopNQueue still respects the actual limitGlobal.
        int initialCapacity = (int) Math.max(1L, Math.min(limitGlobal, Integer.MAX_VALUE));
        PriorityQueue<Pair<Long, Float>> topNQueue = new PriorityQueue<>(
                initialCapacity,
                Comparator.comparing(pair -> pair.second)
        );
        addToTopNQueue(topNQueue, limitGlobal, toRowIdWithScoreList(globalIndexResult));
        return new GlobalIndexResultAggregator() {
            @Override
            public void iterate(GlobalIndexResult partial) {
                List<Pair<Long, Float>> partialList = toRowIdWithScoreList((ScoredGlobalIndexResult) partial);
                addToTopNQueue(topNQueue, limitGlobal, partialList);
            }

            @Override
            public GlobalIndexResult terminate() {
                RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
                Map<Long, Float> scoreMap = new HashMap<>();
                topNQueue.forEach(pair -> {
                    bitmap.add(pair.first);
                    scoreMap.put(pair.first, pair.second);
                });
                bitmap.runOptimize();
                return ScoredGlobalIndexResult.create(() -> bitmap, scoreMap::get);
            }
        };
    }

    public static List<Pair<Long, Float>> toRowIdWithScoreList(ScoredGlobalIndexResult indexResult) {
        RoaringNavigableMap64 results = indexResult.results();
        List<Pair<Long, Float>> rowIdWithScore = new ArrayList<>(results.getIntCardinality());
        Iterator<Long> iterator = results.iterator();
        ScoreGetter scoreGetter = indexResult.scoreGetter();
        while (iterator.hasNext()) {
            Long rowId = iterator.next();
            rowIdWithScore.add(Pair.create(rowId, scoreGetter.score(rowId)));
        }
        return rowIdWithScore;
    }

    public interface GlobalIndexResultAggregator {

        void iterate(GlobalIndexResult result);

        GlobalIndexResult terminate();
    }

}
