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

#include "exec/paimon/paimon_native_reader.h"

#include "arrow/c/bridge.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/parquet_scanner.h"
#include "exec/paimon/paimon_context_factory.h"
#include "exec/tracked_paimon_memory_pool.h"
#include "fs/paimon/paimon_file_system.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/table_read.h"
#include "runtime/descriptors.h"
#include "util/arrow/row_batch.h"

namespace starrocks {

PaimonNativeReader::PaimonNativeReader(const HdfsScannerParams& scanner_params,
                                       const HdfsScannerContext& scanner_ctx,
                                       const TCloudConfiguration& cloud_conf, RuntimeState* state,
                                       HdfsScanStats* fs_stats, HdfsScanStats* app_stats)
        : _scanner_params(scanner_params),
          _scanner_ctx(scanner_ctx),
          _cloud_conf(cloud_conf),
          _fs_stats(fs_stats),
          _app_stats(app_stats),
          _tracked_pool(std::make_shared<TrackedPaimonMemoryPool>(state->query_mem_tracker_ptr().get())),
          _max_chunk_size(state->chunk_size() ? state->chunk_size() : 4096) {
    _init_read_fields();
    _cast_exprs.assign(_scanner_ctx.materialized_columns.size(), nullptr);
    _conv_ctx.state = state;
}

std::shared_ptr<PaimonFileSystem> PaimonNativeReader::_create_paimon_fs() {
    return std::make_shared<PaimonFileSystem>(_scanner_params.paimon_table_path, _cloud_conf,
                                              _scanner_params.datacache_options, _fs_stats, _app_stats);
}

Status PaimonNativeReader::open() {
    SCOPED_RAW_TIMER(&_app_stats->reader_init_ns);
    _paimon_fs = _create_paimon_fs();

    PaimonContextFactory::ReadOptions options{_scanner_params, _scanner_ctx, _field_names,
                                              _tracked_pool, _paimon_fs, _max_batch_rows};
    auto read_context_result = PaimonContextFactory::build_read_context(options);
    if (UNLIKELY(!read_context_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon ReadContextBuilder error: {}", read_context_result.status().ToString()));
    }

    auto split_result = paimon::DataSplit::Deserialize(_scanner_params.paimon_split_info.data(),
                                                       _scanner_params.paimon_split_info.size(), _tracked_pool);
    if (UNLIKELY(!split_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon DataSplit::Deserialize error: {}", split_result.status().ToString()));
    }

    auto table_read_result = paimon::TableRead::Create(std::move(read_context_result).value());
    if (UNLIKELY(!table_read_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon TableRead::Create error: {}", table_read_result.status().ToString()));
    }

    auto reader_result = table_read_result.value()->CreateReader(split_result.value());
    if (UNLIKELY(!reader_result.ok())) {
        return Status::InternalError(fmt::format("Paimon CreateReader error: {}", reader_result.status().ToString()));
    }
    _reader = std::move(reader_result).value();
    _chunk_filter.reserve(0);
    _batch_start_idx = 0;
    _chunk_start_idx = 0;
    _scanner_eof = false;

    // Build conv_funcs + cast_exprs + _read_chunk once. Arrow type is derived from
    // slot type (paimon-cpp handles schema evolution before emitting batches, so
    // slot-derived arrow type matches the batch's array type by construction).
    // Avoids rebuilding the converter chain on every get_next() / batch boundary.
    _read_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_arrow_type(slot_desc->type(), &arrow_type));
        ColumnPtr column;
        RETURN_IF_ERROR(ParquetScanner::new_column(arrow_type.get(), slot_desc, &column,
                                                    _conv_funcs[i].get(), &_cast_exprs[i], _pool, true));
        column->reserve(_max_chunk_size);
        _read_chunk->append_column(column, slot_desc->id());
    }
    return Status::OK();
}

Status PaimonNativeReader::get_next(ChunkPtr* chunk) {
    // Parquet-style staging: _read_chunk is the internal working buffer whose
    // Column objects are reused across calls. We reset its row counts at entry,
    // fill it from arrow batches, then in _fill_dst_chunk swap the data into
    // *chunk's freshly-cloned Columns. _read_chunk's Columns end up empty after
    // each call — safe to reset() on the next call.
    _read_chunk->reset();
    _chunk_filter.clear();

    if (_batch_is_exhausted()) {
        while (true) {
            Status status = _next_batch();
            if (_scanner_eof) {
                return status;
            }
            if (status.ok()) {
                break;
            }
            return status;
        }
    }
    while (!_scanner_eof) {
        RETURN_IF_ERROR(_append_batch_to_read_chunk());
        if (_chunk_is_full()) {
            break;
        }
        auto status = _next_batch();
        if (status.ok()) {
            continue;
        }
        if (!status.is_end_of_file()) {
            return status;
        }
        if (_read_chunk->num_rows() > 0) {
            break;
        }
        return status;
    }
    *chunk = _read_chunk->clone_empty_with_slot(_max_chunk_size);
    RETURN_IF_ERROR(_fill_dst_chunk(chunk));
    _chunk_start_idx = 0;
    return Status::OK();
}

Status PaimonNativeReader::_next_batch() {
    SCOPED_RAW_TIMER(&_app_stats->column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats->io_ns);
    _app_stats->io_count += 1;
    _batch_start_idx = 0;
    auto status = _reader->NextBatch();
    if (status.ok()) {
        auto& c_array = status.value().first;
        auto& c_schema = status.value().second;
        if (!c_array) {
            _scanner_eof = true;
            return Status::EndOfFile("no data");
        }
        auto arrow_batch_result = arrow::ImportRecordBatch(c_array.get(), c_schema.get());
        if (UNLIKELY(!arrow_batch_result.ok())) {
            return Status::InternalError(
                    fmt::format("Arrow ImportRecordBatch : {}", arrow_batch_result.status().ToString()));
        }
        _arrow_batch = arrow_batch_result.ValueOrDie();
        return Status::OK();
    }
    return Status::InternalError(fmt::format("Paimon NextBatch error : {}", status.status().ToString()));
}

Status PaimonNativeReader::_append_batch_to_read_chunk() {
    SCOPED_RAW_TIMER(&_app_stats->column_convert_ns);
    size_t num_elements =
            std::min<size_t>((_max_chunk_size - _chunk_start_idx), (_arrow_batch->num_rows() - _batch_start_idx));
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.current_slot = slot_desc;
        auto* array = _arrow_batch->GetColumnByName(slot_desc->col_name()).get();
        auto& column = _read_chunk->get_column_by_slot_id(slot_desc->id());
        RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(_conv_funcs[i].get(), num_elements, array, column,
                                                                _batch_start_idx, _chunk_start_idx, &_chunk_filter,
                                                                &_conv_ctx));
    }
    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    _app_stats->raw_rows_read += num_elements;
    return Status::OK();
}

Status PaimonNativeReader::_fill_dst_chunk(ChunkPtr* dst) {
    auto num_rows = _read_chunk->filter(_chunk_filter);
    _app_stats->late_materialize_skip_rows += _chunk_start_idx - num_rows;
    SCOPED_RAW_TIMER(&_app_stats->cast_chunk_ns);
    for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, _read_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), _read_chunk->num_rows(), column);
        // swap_column moves data into the dst Column. For passthrough SlotRef
        // cast, `column` aliases _read_chunk's Column — after swap the data
        // lives in dst and _read_chunk's Column is empty. For real cast,
        // `column` is a fresh Column with cast output — same swap result.
        (*dst)->get_column_by_slot_id(slot_desc->id())->swap_column(*column);
    }
    return Status::OK();
}

void PaimonNativeReader::_init_read_fields() {
    const size_t num_materialized_columns = _scanner_ctx.materialized_columns.size();
    _field_names.reserve(num_materialized_columns);
    _conv_funcs.reserve(num_materialized_columns);
    for (const auto& materialized_column : _scanner_ctx.materialized_columns) {
        _field_names.emplace_back(materialized_column.name());
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
}

bool PaimonNativeReader::_chunk_is_full() const {
    return _chunk_start_idx >= _max_chunk_size;
}

bool PaimonNativeReader::_batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

} // namespace starrocks
