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

#include "paimon_native_writer.h"

#include <algorithm>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "column/chunk.h"
#include "column/column.h"
#include "common/status.h"
#include "connector/utils.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "fs/paimon/paimon_file_system.h"
#include "gutil/strings/split.h"
#include "paimon/commit_message.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/write_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/starrocks_column_to_arrow.h"

namespace starrocks {

const std::string PaimonNativeWriter::PARTITION_DEFAULT_NAME = "partition.default-name";

PaimonNativeWriter::PaimonNativeWriter(PaimonTableDescriptor* paimon_table, const TCloudConfiguration& cloud_conf,
                                       std::vector<ExprContext*> partition_expr, std::vector<ExprContext*> bucket_expr,
                                       std::vector<ExprContext*> output_expr,
                                       std::vector<std::string> data_column_names,
                                       std::vector<std::string> data_column_types,
                                       RuntimeProfile::Counter* convert_timer, bool is_static_partition_sink,
                                       std::vector<std::string> partition_column_names,
                                       std::vector<std::string> partition_column_values)
        : _paimon_table(paimon_table),
          _cloud_conf(cloud_conf),
          _output_expr(std::move(output_expr)),
          _partition_expr(std::move(partition_expr)),
          _bucket_expr(std::move(bucket_expr)),
          _data_column_names(std::move(data_column_names)),
          _data_column_types(std::move(data_column_types)),
          _convert_timer(convert_timer),
          _is_static_partition_sink(is_static_partition_sink),
          _partition_column_names(std::move(partition_column_names)),
          _partition_column_values(std::move(partition_column_values)) {}

PaimonNativeWriter::~PaimonNativeWriter() = default;

Status PaimonNativeWriter::do_init(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(get_arrow_schema(runtime_state->timezone()));

    std::map<std::string, std::string> paimon_options = _paimon_table->get_paimon_options();
    const std::string root_path = paimon_options.at(PaimonOptions::ROOT_PATH);
    auto it = paimon_options.find(PARTITION_DEFAULT_NAME);
    if (it != paimon_options.end()) {
        _partition_default_value = it->second;
    }

    std::string paimon_native_commit_user = runtime_state->query_options().paimon_native_commit_user;
    if (paimon_native_commit_user.empty()) {
        paimon_native_commit_user = "root";
    }

    paimon::WriteContextBuilder builder(root_path, paimon_native_commit_user);
    // Inject a PaimonFileSystem built directly from the FE-supplied TCloudConfiguration so we
    // do not have to splice cloud_properties into the option map. The global
    // PaimonFileSystemFactory path was disabled in the reader refactor.
    paimon::Result<std::unique_ptr<paimon::WriteContext>> result =
            builder.SetOptions(paimon_options)
                    .AddOption(PaimonOptions::ROOT_PATH, root_path)
                    .WithFileSystem(std::make_shared<PaimonFileSystem>(root_path, _cloud_conf,
                                                                       DataCacheOptions{}))
                    .WithWriteId(0)
                    .Finish();
    if (!result.ok()) {
        return Status::InternalError(
                fmt::format("Failed to build Paimon WriteContext, reason: {}", result.status().ToString()));
    }
    return create_file_store_write(std::move(result).value());
}

Status PaimonNativeWriter::write(RuntimeState* runtime_state, const ChunkPtr& chunk) {
    if (chunk == nullptr) {
        return Status::InvalidArgument("Chunk cannot be null");
    }

    if (chunk->num_rows() == 0) {
        return Status::OK();
    }

    if (_file_store_write == nullptr) {
        RETURN_IF_ERROR(do_init(runtime_state));
    }
    // If no primary keys or bucket num is not set, write the whole chunk directly
    ASSIGN_OR_RETURN(auto record_batch, convert_chunk_to_record_batch(chunk));
    auto st = _file_store_write->Write(std::move(record_batch));
    if (!st.ok()) {
        return Status::InternalError(fmt::format("Failed to write via paimon, reason: {}", st.message()));
    }
    return Status::OK();
}

Status PaimonNativeWriter::commit(RuntimeState* runtime_state) {
    if (_file_store_write == nullptr) {
        return Status::OK();
    }
    auto st = _file_store_write->PrepareCommit();
    if (!st.ok()) {
        return Status::InternalError(fmt::format("Failed to prepare paimon commit, reason: {}", st.status().message()));
    }
    auto res = paimon::CommitMessage::SerializeList(st.value(), paimon::GetDefaultPool());
    if (!res.ok()) {
        return Status::InternalError(
                fmt::format("Failed to serialized commit message, reason: {}", res.status().message()));
    }
    commit_message = std::move(res).value();
    return Status::OK();
}

void PaimonNativeWriter::close(RuntimeState* runtime_state) noexcept {
    if (_file_store_write == nullptr) {
        return;
    }
    auto metrics = _file_store_write->GetMetrics();
    auto st = _file_store_write->Close();
    if (!st.ok()) {
        LOG(ERROR) << "Failed to close paimon writer, reason: " << st.message();
    }
}

void PaimonNativeWriter::set_output_expr(std::vector<ExprContext*> output_expr) {
    _output_expr = output_expr;
}

std::string PaimonNativeWriter::get_commit_message() {
    return commit_message;
}

Status PaimonNativeWriter::get_arrow_schema(const std::string& timezone) {
    DCHECK(_output_expr.size() == _data_column_names.size());
    DCHECK(_data_column_names.size() == _data_column_types.size());
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    output_fields.reserve(_data_column_names.size());
    for (int i = 0; i < _data_column_names.size(); ++i) {
        auto nullable = _output_expr[i]->root()->is_nullable();
        std::shared_ptr<arrow::DataType> data_type;
        RETURN_IF_ERROR(convert_to_arrow_type(_output_expr[i]->root()->type(), &data_type, timezone));
        auto field = std::make_shared<arrow::Field>(_data_column_names[i], std::move(data_type), nullable);
        output_fields.emplace_back(field);
    }
    _schema = std::make_shared<arrow::Schema>(std::move(output_fields));
    return Status::OK();
}

Status PaimonNativeWriter::create_file_store_write(std::unique_ptr<paimon::WriteContext> context) {
    auto res = paimon::FileStoreWrite::Create(std::move(context));
    if (!res.ok()) {
        return Status::InternalError(fmt::format("Failed to create paimon writer, reason: {}", res.status().message()));
    }
    _file_store_write = std::move(res).value();
    return Status::OK();
}

StatusOr<std::map<std::string, std::string>> PaimonNativeWriter::extract_partition_values(const ChunkPtr& chunk) {
    std::map<std::string, std::string> partition_values;
    const std::vector<std::string>& partition_keys = _paimon_table->get_partition_keys();
    for (int i = 0; i < partition_keys.size(); ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, _partition_expr[i]->evaluate(chunk.get()));
        auto type = _partition_expr[i]->root()->type();
        if (column->has_null() && column->get(0).is_null()) {
            // Use partition.default-name (e.g. "__DEFAULT_PARTITION__") for NULL partition values
            partition_values.emplace(partition_keys[i], _partition_default_value);
        } else {
            ASSIGN_OR_RETURN(auto value, connector::HiveUtils::column_value(type, column, 0));
            partition_values.emplace(partition_keys[i], value);
        }
    }
    return partition_values;
}

StatusOr<std::unique_ptr<paimon::RecordBatch>> PaimonNativeWriter::convert_chunk_to_record_batch(
        const ChunkPtr& chunk) {
    SCOPED_TIMER(_convert_timer);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk.get(), _output_expr, _schema, arrow::default_memory_pool(),
                                                 &record_batch));

    ::ArrowArray arrow_array;
    auto st = arrow::ExportRecordBatch(*record_batch, &arrow_array);
    if (!st.ok()) {
        return Status::InternalError(st.message());
    }
    paimon::RecordBatchBuilder builder(&arrow_array);

    // Set partition information
    if (!_paimon_table->get_partition_keys().empty()) {
        ASSIGN_OR_RETURN(auto partition_values, extract_partition_values(chunk))
        builder.SetPartition(partition_values);
    }
    if (_is_static_partition_sink) {
        std::map<std::string, std::string> partition;
        if (_partition_column_names.size() != _partition_column_values.size()) {
            return Status::InternalError("partition_column_names.size() != partition_column_values.size()");
        }
        for (size_t i = 0; i < _partition_column_names.size(); ++i) {
            partition[_partition_column_names[i]] = _partition_column_values[i];
        }
        builder.SetPartition(partition);
    }

    // Set bucket information
    // After BucketPartitionExchanger shuffle, all rows in this chunk belong to the same bucket,
    // so only compute bucket ID for the first row using zero-copy Slice
    if (_paimon_table->get_bucket_num() > 0) {
        auto first_row_batch = record_batch->Slice(0, 1);
        ASSIGN_OR_RETURN(auto bucket_ids, calculate_bucket_ids(first_row_batch, _paimon_table->get_bucket_num()));
        builder.SetBucket(bucket_ids[0]);
    }

    auto res = builder.Finish();
    if (!res.ok()) {
        return Status::InternalError(res.status().message());
    }
    return std::move(res).value();
}

StatusOr<Buffer<int32_t>> PaimonNativeWriter::calculate_bucket_ids(
        const std::shared_ptr<arrow::RecordBatch>& record_batch, int32_t bucket_num) {
    return connector::PaimonUtils::calculate_bucket_ids(record_batch, _paimon_table->get_bucket_keys(), bucket_num,
                                                        !_paimon_table->get_primary_keys().empty(), _schema);
}

} // namespace starrocks