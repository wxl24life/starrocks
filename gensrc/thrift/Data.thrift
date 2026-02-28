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
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Data.thrift

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Types.thrift"

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TRowBatch {
  // total number of rows contained in this batch
  1: required i32 num_rows

  // row composition
  2: required list<Types.TTupleId> row_tuples

  // There are a total of num_rows * num_tuples_per_row offsets
  // pointing into tuple_data.
  // An offset of -1 records a NULL.
  3: list<i32> tuple_offsets

  // binary tuple data
  // TODO: figure out how we can avoid copying the data during TRowBatch construction
  4: string tuple_data

  // Indicates whether tuple_data is snappy-compressed
  5: bool is_compressed

  // backend num, source
  6: i32 be_number
  // packet seq
  7: i64 packet_seq
}

// this is a union over all possible return types
struct TColumnValue {
  // TODO: use <type>_val instead of camelcase
  1: optional bool boolVal
  2: optional i32 intVal
  3: optional i64 longVal
  4: optional double doubleVal
  5: optional string stringVal
}

struct TResultRow {
  1: list<TColumnValue> colVals
}

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TResultBatch {
  // mysql result row
  1: required list<binary> rows

  // Indicates whether tuple_data is snappy-compressed
  2: required bool is_compressed

  // packet seq used to check if there has packet lost
  3: required i64 packet_seq
  
  // For mark statistic data version
  10: optional i32 statistic_version
}

struct TGlobalDict {
    1: optional i32 columnId
    2: optional list<binary> strings
    3: optional list<i32> ids
    4: optional i64 version
}

// Statistic data for new planner 
struct TStatisticData {
    1: optional string updateTime
    2: optional i64 dbId
    3: optional i64 tableId
    4: optional string columnName
    5: optional i64 rowCount
    6: optional double dataSize
    7: optional i64 countDistinct
    8: optional i64 nullCount
    9: optional string max
    10: optional string min
    11: optional string histogram
    // global dict for low cardinality string
    12: optional TGlobalDict dict
    // the latest partition version for this table
    13: optional i64 meta_version
    14: optional i64 partitionId
    // the batch load version
    15: optional binary hll
    16: optional string partitionName
    17: optional i64 collectionSize
}

// Result data for user variable
struct TVariableData {
    1: optional bool isNull
    2: optional binary result
}

struct TIcebergMetadata {
    1: optional i32 content
    2: optional string file_path
    3: optional string file_format
    4: optional i32 spec_id
    5: optional binary partition_data
    6: optional i64 record_count
    7: optional i64 file_size_in_bytes
    8: optional list<i64> split_offsets
    9: optional i32 sort_id
    10: optional list<i32> equality_ids
    11: optional i64 file_sequence_number
    12: optional i64 data_sequence_number
    13: optional binary column_stats;
    14: optional binary key_metadata;
}

struct TPaimonFileMetadata {
    1: optional string partition_name
    2: optional i32 bucket
    3: optional i32 file_kind           // TODO: delete this
    4: optional string file_name
    5: optional i64 file_size
    6: optional i64 row_count
    7: optional i64 delete_row_count
    8: optional i64 schema_id
    9: optional i32 level
    10: optional binary min_key
    11: optional binary max_key
    12: optional binary key_stats_min         // Serialized BinaryRow
    13: optional binary key_stats_max         // Serialized BinaryRow
    14: optional binary key_stats_null_count  // Serialized BinaryArray
    15: optional binary value_stats_min       // Serialized BinaryRow
    16: optional binary value_stats_max       // Serialized BinaryRow
    17: optional binary value_stats_null_count // Serialized BinaryArray
    18: optional i64 creation_time
    19: optional i64 min_sequence_number
    20: optional i64 max_sequence_number
    // Deletion file fields for DeletionVector support
    21: optional string deletion_path
    22: optional i64 deletion_offset
    23: optional i64 deletion_length
    24: optional i64 deletion_cardinality
    25: optional list<string> extra_files
    26: optional binary embedded_file_index
    27: optional i8 file_source               // 0=APPEND, 1=COMPACT
    28: optional list<string> value_stats_cols
    29: optional string external_path
    30: optional i64 first_row_id
    31: optional list<string> write_cols
    32: optional i32 total_buckets
}

struct TPaimonPartitionMetadata {
    1: optional i64 table_id
    3: optional i64 snapshot_id
    4: optional string partition_name
    5: optional binary partition_values
    6: optional i64 row_count
    7: optional i64 data_size
    8: optional i64 file_count
    9: optional binary min_key           // Serialized BinaryRow
    10: optional binary max_key          // Serialized BinaryRow
    11: optional i64 last_file_creation_time
}

struct TPaimonSchemaMetadata {
    1: optional i64 table_id
    2: optional string catalog_name
    3: optional string database_name
    4: optional string table_name
    5: optional string table_uuid
    6: optional i64 begin_snapshot
    7: optional i64 end_snapshot
    8: optional i32 bucket_num
}

// Metadata data for metadata table
struct TMetadataEntry {
    1: optional TIcebergMetadata iceberg_metadata;
    2: optional TPaimonFileMetadata paimon_file_metadata;
    3: optional TPaimonPartitionMetadata paimon_partition_metadata;
    4: optional TPaimonSchemaMetadata paimon_schema_metadata;
}
