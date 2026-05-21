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

#include "exec/paimon/paimon_context_factory.h"

#include <utility>

#include "exec/paimon/paimon_evaluator.h"
#include "fs/paimon/paimon_file_system.h"
#include "runtime/descriptors.h"

namespace starrocks {

void PaimonContextFactory::_add_read_predicate(paimon::ReadContextBuilder* builder,
                                               const HdfsScannerParams& scanner_params,
                                               const HdfsScannerContext& scanner_ctx) {
    std::vector<Expr*> conjuncts;
    for (const auto& entry : scanner_ctx.conjunct_ctxs_by_slot) {
        for (auto* conjunct_ctx : entry.second) {
            conjuncts.push_back(conjunct_ctx->root());
        }
    }

    if (!conjuncts.empty()) {
        PaimonEvaluator evaluator(scanner_params.tuple_desc->slots());
        const auto predicate = evaluator.evaluate(&conjuncts);
        if (predicate != nullptr) {
            VLOG(10) << "Paimon scanner predicate: " << predicate->ToString();
            builder->SetPredicate(predicate);
        }
    }
}

void PaimonContextFactory::_apply_read_options(paimon::ReadContextBuilder* builder, const ReadOptions& options) {
    builder->SetReadSchema(options.field_names);
    builder->WithMemoryPool(options.memory_pool);
    builder->AddOption(paimon::Options::READ_BATCH_SIZE, std::to_string(options.read_batch_size));
    if (options.scanner_params.file_format == THdfsFileFormat::ORC) {
        builder->AddOption(paimon::Options::FILE_FORMAT, "aliorc");
    }
    builder->EnableMultiThreadRowToBatch(
            options.scanner_params.paimon_native_reader_enable_multi_thread_row_to_batch);
    builder->SetRowToBatchThreadNumber(options.scanner_params.paimon_native_reader_row_to_batch_thread_num);
    // PaimonFileSystem already carries the full TCloudConfiguration; injecting it
    // via WithFileSystem replaces the legacy option-map cloud_properties path.
    builder->WithFileSystem(options.paimon_fs);
    // If FE forwarded the table schema JSON, use it to skip the OSS schema read inside
    // paimon-cpp's TableRead::Create (saves ~80ms per scanner).
    if (!options.scanner_params.paimon_table_schema_json.empty()) {
        builder->SetTableSchema(options.scanner_params.paimon_table_schema_json);
    }
    _add_read_predicate(builder, options.scanner_params, options.scanner_ctx);
}

paimon::Result<std::unique_ptr<paimon::ReadContext>> PaimonContextFactory::build_read_context(
        const ReadOptions& options) {
    paimon::ReadContextBuilder builder(options.scanner_params.paimon_table_path);
    _apply_read_options(&builder, options);
    return builder.Finish();
}

} // namespace starrocks
