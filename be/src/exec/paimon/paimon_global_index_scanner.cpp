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

#include "paimon_global_index_scanner.h"

#include <paimon/executor.h>
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/global_index_scan.h>
#include <paimon/utils/row_range_index.h>

#include <string>

#include "fs/paimon/paimon_file_system.h"
#include "global_index_common.h"
#include "paimon_global_index_evaluator.h"

namespace starrocks {

Status PaimonGlobalIndexScanner::do_open(RuntimeState* runtime_state) {
    return Status::OK();
}

void PaimonGlobalIndexScanner::do_close(RuntimeState* runtime_state) noexcept {}

Status PaimonGlobalIndexScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (return_rows > 0) {
        return Status::EndOfFile("End of index result.");
    }

    ASSIGN_OR_RETURN(const auto global_index_result, evaluateGlobalIndex());

    for (const SlotDescriptor* slot : _scanner_params.materialize_slots) {
        if (slot->col_name() == INDEX_RESULT_COLUMN_NAME) {
            // A null global_index_result means this shard had no matches (zero hits / pruned by
            // pre-filter). Emit a NULL row so the FE-side aggregator's visitNull() runs and the
            // overall query returns an empty result instead of failing.
            if (!global_index_result) {
                TypeDescriptor desc;
                desc.type = TYPE_VARBINARY;
                auto col = ColumnHelper::create_column(desc, true);
                col->append_default();
                chunk->get()->append_or_update_column(std::move(col), slot->id());
            } else {
                ASSIGN_OR_RETURN_PAIMON(
                        const auto serialized_bytes,
                        paimon::GlobalIndexResult::Serialize(global_index_result, paimon::GetDefaultPool()));
                TypeDescriptor desc;
                desc.type = TYPE_VARBINARY;
                auto col = ColumnHelper::create_column(desc, true);
                col->append_datum(Slice(serialized_bytes->data(), serialized_bytes->size()));
                chunk->get()->append_or_update_column(std::move(col), slot->id());
            }
        }
    }

    chunk->get()->set_num_rows(++return_rows);
    return Status::OK();
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexScanner::evaluateGlobalIndex() const {
    const int64_t& from = _scanner_params.paimon_global_index_range_from;
    const int64_t& to = _scanner_params.paimon_global_index_range_to;
    const int64_t& shard_id = _scanner_params.paimon_global_index_shard_id;
    const std::string_view& condition = _scanner_params.paimon_global_index_condition;
    const std::string& table_path = _scanner_params.paimon_table_path;

    rapidjson::Document document;
    if (document.Parse(condition.data()).HasParseError()) {
        return Status::InvalidArgument(fmt::format("Failed to parse condition JSON: {}", condition));
    }
    if (!document.IsObject()) {
        return Status::InvalidArgument(fmt::format("Condition JSON is not an object: {}", condition));
    }

    // Native paimon filesystem only — no Counted/FixPath/LocalCache wrappers.
    // Build PaimonFileSystem directly from the FE-supplied TCloudConfiguration so JindoClient
    // can sign OSS requests for DLF / RESTTokenFileIO catalogs.
    std::shared_ptr<paimon::FileSystem> file_system =
            std::make_shared<PaimonFileSystem>(table_path, _cloud_conf);

    std::shared_ptr<paimon::MemoryPool> memory_pool = paimon::GetMemoryPool();

    // GlobalIndexScan is heavy to construct (open file system, fetch index meta). Build it once,
    // together with the row-range filter for this shard, and let the visitor's
    // column_readers_getter only invoke CreateReaders for each predicate node.
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(
            const auto global_index_scan,
            paimon::GlobalIndexScan::Create(table_path, std::nullopt, std::nullopt, {}, file_system,
                                            paimon::GetGlobalDefaultExecutor(), memory_pool),
            fmt::format("create GlobalIndexScan fail, table_path:{}", table_path));
    // CreateRangeScan was removed upstream; the [from, to) row range is now passed to CreateReaders
    // as a RowRangeIndex that limits the scan to this shard's row ids.
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(
            auto row_range_index, paimon::RowRangeIndex::Create({paimon::Range(from, to)}),
            fmt::format("create RowRangeIndex fail, from:{}, to:{}", from, to));
    const std::optional<paimon::RowRangeIndex> row_range(std::move(row_range_index));

    auto column_readers_getter = [&](const std::string_view column_name)
            -> StatusOr<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>> {
        ASSIGN_OR_RETURN_WITH_MSG_PAIMON(
                auto readers, global_index_scan->CreateReaders(std::string(column_name), row_range),
                fmt::format("create Readers fail, column:{}", column_name));
        return readers;
    };

    std::shared_ptr<paimon::GlobalIndexResult> predicate_index_result;
    if (document.HasMember("predicate")) {
        PaimonGlobalIndexPredicateEvaluator evaluator(column_readers_getter);
        ASSIGN_OR_RETURN(predicate_index_result, visitIndexConditionNode(document["predicate"], evaluator));
    }

    std::shared_ptr<paimon::GlobalIndexResult> global_index_result;
    if (document.HasMember("scoreExpr")) {
        if (!document.HasMember("n") || !document["n"].IsArray() ||
            shard_id < 0 || shard_id >= static_cast<int64_t>(document["n"].Size())) {
            return Status::InvalidArgument(fmt::format(
                    "topN n[] missing or shard_id out of range: shard_id={}, n_size={}",
                    shard_id, document.HasMember("n") && document["n"].IsArray() ? document["n"].Size() : 0));
        }
        const int32_t n = document["n"][shard_id].Get<int32_t>();
        auto bitmap_result = std::dynamic_pointer_cast<paimon::BitmapGlobalIndexResult>(predicate_index_result);
        PaimonGlobalIndexTopNEvaluator evaluator(column_readers_getter, bitmap_result, n);
        ASSIGN_OR_RETURN(global_index_result, visitIndexConditionNode(document["scoreExpr"], evaluator));
    } else {
        global_index_result = predicate_index_result;
    }

    if (global_index_result) {
        ASSIGN_OR_RETURN_PAIMON(global_index_result, global_index_result->AddOffset(from));
    }
    return global_index_result;
}

Status PaimonGlobalIndexScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return_rows = 0;
    return Status::OK();
}

void PaimonGlobalIndexScanner::do_update_counter(HdfsScanProfile* profile) {}

} // namespace starrocks
