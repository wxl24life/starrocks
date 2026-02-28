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

#include "runtime/metadata_result_writer.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "util/thrift_util.h"

namespace starrocks {

MetadataResultWriter::MetadataResultWriter(BufferControlBlock* sinker,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           RuntimeProfile* parent_profile, TResultSinkType::type sink_type,
                                           TPaimonMetadataType::type paimon_metadata_type)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),
          _sink_type(sink_type),
          _paimon_metadata_type(paimon_metadata_type) {}

MetadataResultWriter::~MetadataResultWriter() = default;

Status MetadataResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

Status MetadataResultWriter::append_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok() || process_status.value() == nullptr) {
        return process_status.status();
    }
    auto result = std::move(process_status.value());

    const size_t num_rows = result->result_batch.rows.size();
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append metadata result to sink failed.";
    return status;
}

StatusOr<TFetchDataResultPtrs> MetadataResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    TFetchDataResultPtrs results;
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok()) {
        return process_status.status();
    }
    if (process_status.value() != nullptr) {
        results.push_back(std::move(process_status.value()));
    }
    return results;
}

StatusOr<TFetchDataResultPtr> MetadataResultWriter::_process_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    const int num_columns = _output_expr_ctxs.size();

    Columns result_columns;
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        result_columns.emplace_back(std::move(col));
    }

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    if (_sink_type == TResultSinkType::METADATA_ICEBERG) {
        RETURN_IF_ERROR(_fill_iceberg_metadata(result_columns, chunk, result.get()));
    } else if (_sink_type == TResultSinkType::METADATA_PAIMON) {
        switch (_paimon_metadata_type) {
        case TPaimonMetadataType::FILE_METADATA:
            RETURN_IF_ERROR(_fill_paimon_file_metadata(result_columns, chunk, result.get()));
            break;
        case TPaimonMetadataType::PARTITION_METADATA:
            RETURN_IF_ERROR(_fill_paimon_partition_metadata(result_columns, chunk, result.get()));
            break;
        case TPaimonMetadataType::SCHEMA_METADATA:
            RETURN_IF_ERROR(_fill_paimon_schema_metadata(result_columns, chunk, result.get()));
            break;
        default:
            return Status::InternalError(fmt::format("Unknown Paimon metadata type: {}", _paimon_metadata_type));
        }
    }

    return result;
}

// Columns is fixed in the first version of logical iceberg metadata table.
// In principle, there will be no changes in the future.
// Only new columns are allowed if necessary. The newly added columns need to check if nullable.
// The first version position -> column_name
// 0 -> "content"
// 1 -> "file_path"
// 2 -> "file_format"
// 3 -> "spec_id"
// 4 -> "partition_data"
// 5 -> "record_count"
// 6 -> "file_size_in_bytes"
// 7 -> "split_offsets"
// 8 -> "sort_id"
// 9 -> "equality_ids"
// 10 -> "file_sequence_number"
// 11 -> "data_sequence_number"
// 12 -> "column_stats"
// 13 -> "key_metadata"
Status MetadataResultWriter::_fill_iceberg_metadata(const Columns& columns, const Chunk* chunk,
                                                    TFetchDataResult* result) const {
    SCOPED_TIMER(_convert_tuple_timer);

    const auto* content = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[0].get()));
    const auto* file_path = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1].get()));
    const auto* file_format = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2].get()));
    const auto* spec_id = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[3].get()));
    const auto* partition_data = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));
    const auto* record_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* file_size_in_bytes = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* split_offsets = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[7].get()));

    const auto* sort_id = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[8].get()));
    const auto* equality_ids = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    const auto* file_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[10].get()));
    const auto* data_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[11].get()));
    const auto* iceberg_metrics = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[12].get()));
    const auto* key_metadata = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[13].get()));

    std::vector<TMetadataEntry> meta_entries;
    int num_rows = chunk->num_rows();

    meta_entries.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        TIcebergMetadata iceberg_metadata;
        meta_entries[i].__set_iceberg_metadata(iceberg_metadata);
        auto& iceberg_meta = meta_entries[i].iceberg_metadata;

        iceberg_meta.__set_content(content->get(i).get_int32());
        iceberg_meta.__set_file_path(file_path->get_slice(i).to_string());
        iceberg_meta.__set_file_format(file_format->get_slice(i).to_string());
        iceberg_meta.__set_spec_id(spec_id->get(i).get_int32());

        if (!columns[4]->is_null(i)) {
            iceberg_meta.__set_partition_data(partition_data->get_slice(i).to_string());
        }

        iceberg_meta.__set_record_count(record_count->get(i).get_int64());
        iceberg_meta.__set_file_size_in_bytes(file_size_in_bytes->get(i).get_int64());

        std::vector<int64_t> offsets;
        const auto split_array = split_offsets->get(i).get_array();
        for (auto& split_offset : split_array) {
            offsets.emplace_back(split_offset.get_int64());
        }
        iceberg_meta.__set_split_offsets(offsets);

        if (!columns[8]->is_null(i)) {
            iceberg_meta.__set_sort_id(sort_id->get(i).get_int32());
        }
        if (!columns[9]->is_null(i)) {
            std::vector<int32_t> ids;
            const auto eq_id_array = equality_ids->get(i).get_array();
            for (auto& eq_id : eq_id_array) {
                ids.emplace_back(eq_id.get_int32());
            }
            iceberg_meta.__set_equality_ids(ids);
        }
        if (!columns[10]->is_null(i)) {
            iceberg_meta.__set_file_sequence_number(file_sequence_number->get(i).get_int64());
        }
        if (!columns[11]->is_null(i)) {
            iceberg_meta.__set_data_sequence_number(data_sequence_number->get(i).get_int64());
        }
        if (!columns[12]->is_null(i)) {
            iceberg_meta.__set_column_stats(iceberg_metrics->get_slice(i).to_string());
        }
        if (!columns[13]->is_null(i)) {
            iceberg_meta.__set_key_metadata(key_metadata->get_slice(i).to_string());
        }
    }

    result->result_batch.rows.resize(num_rows);

    ThriftSerializer serializer(false, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&meta_entries[i], &result->result_batch.rows[i]));
    }

    return Status::OK();
}

// Columns for Paimon file_statistics query (32 columns):
// 0 -> "partition_name"
// 1 -> "bucket"
// 2 -> "total_buckets"
// 3 -> "file_kind"
// 4 -> "file_name"
// 5 -> "file_size"
// 6 -> "row_count"
// 7 -> "min_key"
// 8 -> "max_key"
// 9 -> "key_stats_min"
// 10 -> "key_stats_max"
// 11 -> "key_stats_null_count"
// 12 -> "value_stats_min"
// 13 -> "value_stats_max"
// 14 -> "value_stats_null_count"
// 15 -> "min_sequence_number"
// 16 -> "max_sequence_number"
// 17 -> "schema_id"
// 18 -> "level"
// 19 -> "extra_files"
// 20 -> "creation_time"
// 21 -> "delete_row_count"
// 22 -> "embedded_file_index"
// 23 -> "file_source"
// 24 -> "value_stats_cols"
// 25 -> "external_path"
// 26 -> "first_row_id"
// 27 -> "write_cols"
// 28 -> "deletion_path"
// 29 -> "deletion_offset"
// 30 -> "deletion_length"
// 31 -> "deletion_cardinality"
Status MetadataResultWriter::_fill_paimon_file_metadata(const Columns& columns, const Chunk* chunk,
                                                        TFetchDataResult* result) const {
    SCOPED_TIMER(_convert_tuple_timer);
    // NOT NULL columns
    const auto* partition_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    const auto* bucket = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[1].get()));
    const auto* total_buckets = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[2].get()));
    const auto* file_kind = down_cast<const Int8Column*>(ColumnHelper::get_data_column(columns[3].get()));
    const auto* file_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));
    const auto* file_size = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* row_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* min_key = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[7].get()));
    const auto* max_key = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[8].get()));
    const auto* key_stats_min = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    const auto* key_stats_max = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[10].get()));
    const auto* key_stats_null_count = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[11].get()));
    const auto* value_stats_min = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[12].get()));
    const auto* value_stats_max = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[13].get()));
    const auto* value_stats_null_count = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[14].get()));
    const auto* min_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[15].get()));
    const auto* max_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[16].get()));
    const auto* schema_id = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[17].get()));
    const auto* level = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[18].get()));
    const auto* extra_files = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[19].get()));
    const auto* creation_time = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[20].get()));
    // NULLABLE columns
    const auto* delete_row_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[21].get()));
    const auto* embedded_file_index = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[22].get()));
    const auto* file_source = down_cast<const Int8Column*>(ColumnHelper::get_data_column(columns[23].get()));
    const auto* value_stats_cols = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[24].get()));
    const auto* external_path = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[25].get()));
    const auto* first_row_id = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[26].get()));
    const auto* write_cols = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[27].get()));
    // Deletion file fields
    const auto* deletion_path = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[28].get()));
    const auto* deletion_offset = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[29].get()));
    const auto* deletion_length = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[30].get()));
    const auto* deletion_cardinality = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[31].get()));

    std::vector<TMetadataEntry> meta_entries;
    int num_rows = chunk->num_rows();

    meta_entries.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        TPaimonFileMetadata paimon_file_metadata;
        meta_entries[i].__set_paimon_file_metadata(paimon_file_metadata);
        auto& paimon_meta = meta_entries[i].paimon_file_metadata;

        // NOT NULL fields
        paimon_meta.__set_partition_name(partition_name->get_slice(i).to_string());
        paimon_meta.__set_bucket(bucket->get(i).get_int32());
        paimon_meta.__set_total_buckets(total_buckets->get(i).get_int32());
        paimon_meta.__set_file_kind(file_kind->get(i).get_int8());
        paimon_meta.__set_file_name(file_name->get_slice(i).to_string());
        paimon_meta.__set_file_size(file_size->get(i).get_int64());
        paimon_meta.__set_row_count(row_count->get(i).get_int64());
        paimon_meta.__set_min_key(min_key->get_slice(i).to_string());
        paimon_meta.__set_max_key(max_key->get_slice(i).to_string());
        paimon_meta.__set_key_stats_min(key_stats_min->get_slice(i).to_string());
        paimon_meta.__set_key_stats_max(key_stats_max->get_slice(i).to_string());
        paimon_meta.__set_key_stats_null_count(key_stats_null_count->get_slice(i).to_string());
        paimon_meta.__set_value_stats_min(value_stats_min->get_slice(i).to_string());
        paimon_meta.__set_value_stats_max(value_stats_max->get_slice(i).to_string());
        paimon_meta.__set_value_stats_null_count(value_stats_null_count->get_slice(i).to_string());
        paimon_meta.__set_min_sequence_number(min_sequence_number->get(i).get_int64());
        paimon_meta.__set_max_sequence_number(max_sequence_number->get(i).get_int64());
        paimon_meta.__set_schema_id(schema_id->get(i).get_int64());
        paimon_meta.__set_level(level->get(i).get_int32());
        // extra_files (ARRAY<VARCHAR>)
        {
            std::vector<std::string> files;
            const auto arr = extra_files->get(i).get_array();
            for (auto& elem : arr) {
                files.emplace_back(elem.get_slice().to_string());
            }
            paimon_meta.__set_extra_files(files);
        }
        paimon_meta.__set_creation_time(creation_time->get(i).get_int64());

        // NULLABLE fields
        if (!columns[21]->is_null(i)) {
            paimon_meta.__set_delete_row_count(delete_row_count->get(i).get_int64());
        }
        if (!columns[22]->is_null(i)) {
            paimon_meta.__set_embedded_file_index(embedded_file_index->get_slice(i).to_string());
        }
        if (!columns[23]->is_null(i)) {
            paimon_meta.__set_file_source(file_source->get(i).get_int8());
        }
        // value_stats_cols (ARRAY<VARCHAR>)
        if (!columns[24]->is_null(i)) {
            std::vector<std::string> cols;
            const auto arr = value_stats_cols->get(i).get_array();
            for (auto& elem : arr) {
                cols.emplace_back(elem.get_slice().to_string());
            }
            paimon_meta.__set_value_stats_cols(cols);
        }
        if (!columns[25]->is_null(i)) {
            paimon_meta.__set_external_path(external_path->get_slice(i).to_string());
        }
        if (!columns[26]->is_null(i)) {
            paimon_meta.__set_first_row_id(first_row_id->get(i).get_int64());
        }
        // write_cols (ARRAY<VARCHAR>)
        if (!columns[27]->is_null(i)) {
            std::vector<std::string> cols;
            const auto arr = write_cols->get(i).get_array();
            for (auto& elem : arr) {
                cols.emplace_back(elem.get_slice().to_string());
            }
            paimon_meta.__set_write_cols(cols);
        }

        // Deletion file fields (NULLABLE)
        if (!columns[28]->is_null(i)) {
            paimon_meta.__set_deletion_path(deletion_path->get_slice(i).to_string());
        }
        if (!columns[29]->is_null(i)) {
            paimon_meta.__set_deletion_offset(deletion_offset->get(i).get_int64());
        }
        if (!columns[30]->is_null(i)) {
            paimon_meta.__set_deletion_length(deletion_length->get(i).get_int64());
        }
        if (!columns[31]->is_null(i)) {
            paimon_meta.__set_deletion_cardinality(deletion_cardinality->get(i).get_int64());
        }
    }

    result->result_batch.rows.resize(num_rows);

    ThriftSerializer serializer(false, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&meta_entries[i], &result->result_batch.rows[i]));
    }

    return Status::OK();
}

// Columns for Paimon partition_statistics query (10 columns):
// 0 -> "table_id"
// 1 -> "snapshot_id"
// 2 -> "partition_name"
// 3 -> "partition_values" (VARBINARY)
// 4 -> "row_count"
// 5 -> "data_size"
// 6 -> "file_count"
// 7 -> "min_key" (VARBINARY, optional)
// 8 -> "max_key" (VARBINARY, optional)
// 9 -> "last_file_creation_time"
Status MetadataResultWriter::_fill_paimon_partition_metadata(const Columns& columns, const Chunk* chunk,
                                                             TFetchDataResult* result) const {
    SCOPED_TIMER(_convert_tuple_timer);

    const auto* table_id = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[0].get()));
    const auto* snapshot_id = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1].get()));
    const auto* partition_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2].get()));
    const auto* partition_values = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[3].get()));
    const auto* row_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[4].get()));
    const auto* data_size = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* file_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* min_key = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[7].get()));
    const auto* max_key = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[8].get()));
    const auto* last_creation_time = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[9].get()));

    std::vector<TMetadataEntry> meta_entries;
    int num_rows = chunk->num_rows();
    meta_entries.resize(num_rows);

    for (int i = 0; i < num_rows; ++i) {
        TPaimonPartitionMetadata partition_metadata;
        meta_entries[i].__set_paimon_partition_metadata(partition_metadata);
        auto& partition_meta = meta_entries[i].paimon_partition_metadata;

        partition_meta.__set_table_id(table_id->get(i).get_int64());
        partition_meta.__set_snapshot_id(snapshot_id->get(i).get_int64());
        partition_meta.__set_partition_name(partition_name->get_slice(i).to_string());
        partition_meta.__set_partition_values(partition_values->get_slice(i).to_string());

        partition_meta.__set_row_count(row_count->get(i).get_int64());
        partition_meta.__set_data_size(data_size->get(i).get_int64());
        partition_meta.__set_file_count(file_count->get(i).get_int64());

        // Optional fields
        if (!columns[7]->is_null(i)) {
            partition_meta.__set_min_key(min_key->get_slice(i).to_string());
        }
        if (!columns[8]->is_null(i)) {
            partition_meta.__set_max_key(max_key->get_slice(i).to_string());
        }

        partition_meta.__set_last_file_creation_time(last_creation_time->get(i).get_int64());
    }

    result->result_batch.rows.resize(num_rows);
    ThriftSerializer serializer(false, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&meta_entries[i], &result->result_batch.rows[i]));
    }

    return Status::OK();
}

// Columns for Paimon table_schema table (8 columns) - PRIMARY KEY table:
// 0 -> "catalog_name" (PK)
// 1 -> "database_name" (PK)
// 2 -> "table_name" (PK)
// 3 -> "table_id"
// 4 -> "table_uuid"
// 5 -> "begin_snapshot"
// 6 -> "end_snapshot"
// 7 -> "bucket_num"
Status MetadataResultWriter::_fill_paimon_schema_metadata(const Columns& columns, const Chunk* chunk,
                                                          TFetchDataResult* result) const {
    SCOPED_TIMER(_convert_tuple_timer);

    const auto* catalog_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    const auto* database_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1].get()));
    const auto* table_name = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2].get()));
    const auto* table_id = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[3].get()));
    const auto* table_uuid = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));
    const auto* begin_snapshot = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* end_snapshot = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* bucket_num = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[7].get()));

    std::vector<TMetadataEntry> meta_entries;
    int num_rows = chunk->num_rows();
    meta_entries.resize(num_rows);

    for (int i = 0; i < num_rows; ++i) {
        TPaimonSchemaMetadata schema_metadata;
        meta_entries[i].__set_paimon_schema_metadata(schema_metadata);
        auto& schema_meta = meta_entries[i].paimon_schema_metadata;

        schema_meta.__set_catalog_name(catalog_name->get_slice(i).to_string());
        schema_meta.__set_database_name(database_name->get_slice(i).to_string());
        schema_meta.__set_table_name(table_name->get_slice(i).to_string());
        schema_meta.__set_table_id(table_id->get(i).get_int64());
        schema_meta.__set_table_uuid(table_uuid->get_slice(i).to_string());
        schema_meta.__set_begin_snapshot(begin_snapshot->get(i).get_int64());
        schema_meta.__set_end_snapshot(end_snapshot->get(i).get_int64());
        schema_meta.__set_bucket_num(bucket_num->get(i).get_int32());
    }

    result->result_batch.rows.resize(num_rows);
    ThriftSerializer serializer(false, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&meta_entries[i], &result->result_batch.rows[i]));
    }

    return Status::OK();
}
} // namespace starrocks