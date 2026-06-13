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
#include <paimon/global_index/global_index_reader_cache.h>
#include <paimon/global_index/global_index_scan.h>
#include <paimon/global_index/lumina/lumina_options.h>
#include <paimon/utils/row_range_index.h>

#include <string>

#include "cache/block_cache/cache_options.h"
#include "common/config.h"
#include "fs/paimon/paimon_file_system.h"
#include "global_index_common.h"
#include "paimon_global_index_evaluator.h"
#include "runtime/runtime_state.h"
#include "util/stopwatch.hpp"
#include "util/uid_util.h"

namespace starrocks {

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> EvaluatePaimonGlobalIndex(
        const std::string& table_path, const TCloudConfiguration& cloud_conf,
        const DataCacheOptions& datacache_options, std::string_view condition, int64_t range_from, int64_t range_to,
        int64_t shard_id) {
    // Push R11/R4/R5 toggles from BE configs into paimon-cpp static state on every entry —
    // identical to PaimonGlobalIndexScanner::evaluateGlobalIndex so the bypass and SQL paths
    // share the same cache behaviour.
    const int32_t cache_cap_cfg = config::paimon_global_index_reader_cache_capacity;
    const int32_t readahead_kb_cfg = config::paimon_lumina_file_reader_readahead_kb;
    paimon::GlobalIndexReaderCache::Instance().SetCapacity(cache_cap_cfg > 0 ? static_cast<size_t>(cache_cap_cfg) : 0);
    paimon::lumina::SetLuminaFileReaderReadAheadBytes(
            readahead_kb_cfg > 0 ? static_cast<uint64_t>(readahead_kb_cfg) * 1024 : 0);
    paimon::SetScanMetadataCacheTtlMs(config::paimon_scan_metadata_cache_ttl_ms);
    paimon::SetManifestScanCacheTtlMs(config::paimon_manifest_scan_cache_ttl_ms);

    rapidjson::Document document;
    if (document.Parse(condition.data(), condition.size()).HasParseError()) {
        return Status::InvalidArgument(fmt::format("Failed to parse condition JSON: {}", condition));
    }
    if (!document.IsObject()) {
        return Status::InvalidArgument(fmt::format("Condition JSON is not an object: {}", condition));
    }

    // No fs_stats/app_stats: the bypass path doesn't surface a HdfsScanProfile, so the IO
    // counters that PaimonFileSystem populates would be unread anyway. nullptr is allowed.
    std::shared_ptr<PaimonFileSystem> paimon_fs =
            std::make_shared<PaimonFileSystem>(table_path, cloud_conf, datacache_options, nullptr, nullptr);
    std::shared_ptr<paimon::FileSystem> file_system = paimon_fs;
    std::shared_ptr<paimon::MemoryPool> memory_pool = paimon::GetMemoryPool();

    paimon::ScanCreateBreakdown scan_breakdown;
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(const auto global_index_scan,
                                     paimon::GlobalIndexScan::Create(table_path, std::nullopt, std::nullopt, {},
                                                                     file_system, paimon::GetGlobalDefaultExecutor(),
                                                                     memory_pool, &scan_breakdown),
                                     fmt::format("create GlobalIndexScan fail, table_path:{}", table_path));

    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(
            auto row_range_index, paimon::RowRangeIndex::Create({paimon::Range(range_from, range_to)}),
            fmt::format("create RowRangeIndex fail, from:{}, to:{}", range_from, range_to));
    const std::optional<paimon::RowRangeIndex> row_range(std::move(row_range_index));

    auto column_readers_getter = [&](const std::string_view column_name)
            -> StatusOr<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>> {
        ASSIGN_OR_RETURN_WITH_MSG_PAIMON(auto readers,
                                         global_index_scan->CreateReaders(std::string(column_name), row_range),
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
        if (!document.HasMember("n") || !document["n"].IsArray() || shard_id < 0 ||
            shard_id >= static_cast<int64_t>(document["n"].Size())) {
            return Status::InvalidArgument(
                    fmt::format("topN n[] missing or shard_id out of range: shard_id={}, n_size={}", shard_id,
                                document.HasMember("n") && document["n"].IsArray() ? document["n"].Size() : 0));
        }
        const int32_t topn_value = document["n"][shard_id].Get<int32_t>();
        auto bitmap_result = std::dynamic_pointer_cast<paimon::BitmapGlobalIndexResult>(predicate_index_result);
        PaimonGlobalIndexTopNEvaluator evaluator(column_readers_getter, bitmap_result, topn_value);
        ASSIGN_OR_RETURN(global_index_result, visitIndexConditionNode(document["scoreExpr"], evaluator));
    } else {
        global_index_result = predicate_index_result;
    }

    if (global_index_result) {
        ASSIGN_OR_RETURN_PAIMON(global_index_result, global_index_result->AddOffset(range_from));
    }
    return global_index_result;
}

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
                MonotonicStopWatch serialize_sw;
                serialize_sw.start();
                ASSIGN_OR_RETURN_PAIMON(
                        const auto serialized_bytes,
                        paimon::GlobalIndexResult::Serialize(global_index_result, paimon::GetDefaultPool()));
                _timings.result_serialize_ns += serialize_sw.elapsed_time();

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

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexScanner::evaluateGlobalIndex() {
    const int64_t& from = _scanner_params.paimon_global_index_range_from;
    const int64_t& to = _scanner_params.paimon_global_index_range_to;
    const int64_t& shard_id = _scanner_params.paimon_global_index_shard_id;
    const std::string_view& condition = _scanner_params.paimon_global_index_condition;
    const std::string& table_path = _scanner_params.paimon_table_path;

    // Push R11 optimization toggles from BE configs into paimon-cpp static state on every
    // query entry. Cheap (atomic store) and lets ADMIN SET tweak behaviour live without
    // BE restart. SetCapacity / SetReadAheadBytes are idempotent; calling per-query is safe.
    const int32_t cache_cap_cfg = config::paimon_global_index_reader_cache_capacity;
    const int32_t readahead_kb_cfg = config::paimon_lumina_file_reader_readahead_kb;
    paimon::GlobalIndexReaderCache::Instance().SetCapacity(cache_cap_cfg > 0 ? static_cast<size_t>(cache_cap_cfg) : 0);
    paimon::lumina::SetLuminaFileReaderReadAheadBytes(
            readahead_kb_cfg > 0 ? static_cast<uint64_t>(readahead_kb_cfg) * 1024 : 0);
    // R4: turn on paimon-cpp Schema+Snapshot cache. Default 0 = off, no behavior change.
    // Setting per-shard is idempotent and cheap (one atomic store on the singleton TTL).
    paimon::SetScanMetadataCacheTtlMs(config::paimon_scan_metadata_cache_ttl_ms);
    // R5: turn on paimon-cpp manifest-scan cache, default 0 = off. Independent A/B knob.
    paimon::SetManifestScanCacheTtlMs(config::paimon_manifest_scan_cache_ttl_ms);

    MonotonicStopWatch total_sw;
    total_sw.start();
    MonotonicStopWatch phase_sw;
    phase_sw.start();
    int32_t topn_value = -1;
    bool has_predicate = false;

    rapidjson::Document document;
    if (document.Parse(condition.data()).HasParseError()) {
        return Status::InvalidArgument(fmt::format("Failed to parse condition JSON: {}", condition));
    }
    if (!document.IsObject()) {
        return Status::InvalidArgument(fmt::format("Condition JSON is not an object: {}", condition));
    }
    _timings.json_parse_ns += phase_sw.reset();

    // Build PaimonFileSystem directly from the FE-supplied TCloudConfiguration so JindoClient
    // can sign OSS requests for DLF / RESTTokenFileIO catalogs.
    //
    // Pass _scanner_params.datacache_options so paimon-cpp's reads of manifest + index files
    // (notably the 4GB DiskANN per shard) get block-cached in StarRocks DataCache.
    //
    // Wire base-class _fs_stats / _app_stats so the base profile machinery surfaces OSS bytes
    // vs post-cache bytes. Held via _paimon_fs so do_update_counter() can later pull
    // get_datacache_stats() / get_shared_buffered_stats().
    _paimon_fs = std::make_shared<PaimonFileSystem>(table_path, _cloud_conf, _scanner_params.datacache_options,
                                                    &_fs_stats, &_app_stats);
    std::shared_ptr<paimon::FileSystem> file_system = _paimon_fs;
    _timings.fs_build_ns += phase_sw.reset();

    std::shared_ptr<paimon::MemoryPool> memory_pool = paimon::GetMemoryPool();

    // GlobalIndexScan is heavy to construct (open file system, fetch index meta). Build it once,
    // together with the row-range filter for this shard, and let the visitor's
    // column_readers_getter only invoke CreateReaders for each predicate node.
    paimon::ScanCreateBreakdown scan_breakdown;
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(
            const auto global_index_scan,
            paimon::GlobalIndexScan::Create(table_path, std::nullopt, std::nullopt, {}, file_system,
                                            paimon::GetGlobalDefaultExecutor(), memory_pool,
                                            &scan_breakdown),
            fmt::format("create GlobalIndexScan fail, table_path:{}", table_path));
    _timings.scan_create_ns += phase_sw.reset();
    _timings.scan_load_schema_ns += scan_breakdown.load_schema_ns;
    _timings.scan_merge_options_ns += scan_breakdown.merge_options_ns;
    _timings.scan_load_snapshot_ns += scan_breakdown.load_snapshot_ns;
    _timings.scan_path_factory_ns += scan_breakdown.path_factory_ns;
    _timings.scan_manifest_create_ns += scan_breakdown.manifest_create_ns;
    _timings.scan_manifest_scan_ns += scan_breakdown.manifest_scan_ns;
    _timings.scan_executor_setup_ns += scan_breakdown.executor_setup_ns;

    // CreateRangeScan was removed upstream; the [from, to) row range is now passed to CreateReaders
    // as a RowRangeIndex that limits the scan to this shard's row ids.
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON(auto row_range_index, paimon::RowRangeIndex::Create({paimon::Range(from, to)}),
                                     fmt::format("create RowRangeIndex fail, from:{}, to:{}", from, to));
    const std::optional<paimon::RowRangeIndex> row_range(std::move(row_range_index));
    _timings.row_range_create_ns += phase_sw.reset();

    auto column_readers_getter = [&, this](const std::string_view column_name)
            -> StatusOr<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>> {
        ASSIGN_OR_RETURN_WITH_MSG_PAIMON(auto readers,
                                         global_index_scan->CreateReaders(std::string(column_name), row_range),
                                         fmt::format("create Readers fail, column:{}", column_name));
        _log_readers_created += static_cast<int32_t>(readers.size());
        return readers;
    };

    std::shared_ptr<paimon::GlobalIndexResult> predicate_index_result;
    if (document.HasMember("predicate")) {
        has_predicate = true;
        // Re-anchor phase timer so predicate eval starts measuring from here, not from the
        // unrelated time spent setting up column_readers_getter above.
        phase_sw.reset();
        PaimonGlobalIndexPredicateEvaluator evaluator(column_readers_getter);
        ASSIGN_OR_RETURN(predicate_index_result, visitIndexConditionNode(document["predicate"], evaluator));
        _timings.predicate_eval_ns += phase_sw.reset();
    }

    std::shared_ptr<paimon::GlobalIndexResult> global_index_result;
    if (document.HasMember("scoreExpr")) {
        if (!document.HasMember("n") || !document["n"].IsArray() || shard_id < 0 ||
            shard_id >= static_cast<int64_t>(document["n"].Size())) {
            return Status::InvalidArgument(
                    fmt::format("topN n[] missing or shard_id out of range: shard_id={}, n_size={}", shard_id,
                                document.HasMember("n") && document["n"].IsArray() ? document["n"].Size() : 0));
        }
        topn_value = document["n"][shard_id].Get<int32_t>();
        phase_sw.reset();
        auto bitmap_result = std::dynamic_pointer_cast<paimon::BitmapGlobalIndexResult>(predicate_index_result);
        PaimonGlobalIndexTopNEvaluator evaluator(column_readers_getter, bitmap_result, topn_value);
        ASSIGN_OR_RETURN(global_index_result, visitIndexConditionNode(document["scoreExpr"], evaluator));
        _timings.score_expr_eval_ns += phase_sw.reset();
    } else {
        global_index_result = predicate_index_result;
    }

    if (global_index_result) {
        phase_sw.reset();
        ASSIGN_OR_RETURN_PAIMON(global_index_result, global_index_result->AddOffset(from));
        _timings.result_add_offset_ns += phase_sw.reset();

        // Count matched rows via ToRanges() for the log line. ToRanges sums Count() from the
        // bitmap's run-length encoding, so it's cheap (no row-by-row iteration).
        // paimon::Range is inclusive [from, to] -> Count() == to - from + 1.
        auto ranges_result = global_index_result->ToRanges();
        if (ranges_result.ok()) {
            int64_t total = 0;
            for (const auto& range : ranges_result.value()) {
                total += range.Count();
            }
            _log_result_rows = total;
        }
    }

    _timings.evaluate_total_ns += total_sw.elapsed_time();

    // Single structured log line per shard scan: identity + outcome. Intentionally NOT in profile
    // (these are per-shard constants, not aggregable measurements). Grep `PaimonGlobalIndexScanner`
    // in BE log to correlate "which BE handled which shard" and what was asked.
    const std::string query_id_str = _runtime_state ? print_id(_runtime_state->query_id()) : "?";
    LOG(INFO) << "PaimonGlobalIndexScanner finished"
              << " query_id=" << query_id_str << " shard_id=" << shard_id << " range=[" << from << "," << to << ")"
              << " topN=" << topn_value << " has_predicate=" << (has_predicate ? 1 : 0)
              << " readers_created=" << _log_readers_created << " result_rows=" << _log_result_rows
              << " eval_time_ms=" << (_timings.evaluate_total_ns / 1'000'000);

    return global_index_result;
}

Status PaimonGlobalIndexScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return_rows = 0;
    _timings = PhaseTimings{};
    _log_readers_created = 0;
    _log_result_rows = 0;
    return Status::OK();
}

void PaimonGlobalIndexScanner::do_update_counter(HdfsScanProfile* profile) {
    // No PaimonFileSystem yet means evaluateGlobalIndex() never ran (e.g. early EOF before
    // do_get_next produced anything). Nothing to surface.
    if (!_paimon_fs) {
        return;
    }

    // Phase-level timers under a "PaimonGlobalIndex" parent section. Always surfaced, even when
    // DataCache is disabled, so a slow scan can be attributed to JSON parse / FS build / scan
    // create / predicate / topN / serialize without diving into BE logs.
    RuntimeProfile* p = profile->runtime_profile;
    const std::string GI_SECTION = "PaimonGlobalIndex";
    ADD_COUNTER(p, GI_SECTION, TUnit::NONE);
    auto add_phase_timer = [&](const char* name, int64_t value_ns) {
        auto* counter = ADD_CHILD_COUNTER(p, name, TUnit::TIME_NS, GI_SECTION);
        COUNTER_SET(counter, value_ns);
    };
    add_phase_timer("EvaluateGlobalIndexTime", _timings.evaluate_total_ns);
    add_phase_timer("JsonParseTime", _timings.json_parse_ns);
    add_phase_timer("FileSystemBuildTime", _timings.fs_build_ns);
    add_phase_timer("GlobalIndexScanCreateTime", _timings.scan_create_ns);
    // Sub-phases of GlobalIndexScanCreateTime (~236ms wall under c=50 contention). Approximate
    // sum of the 7 ScanCreate_* sub-counters equals GlobalIndexScanCreateTime. Surfaced separately
    // to attribute the opaque setup cost to SchemaManager.Latest / SnapshotManager.LatestSnapshot
    // / FileStorePathFactory / IndexManifestFile::Create / index_file_handler->Scan / executor.
    add_phase_timer("ScanCreate_LoadSchemaTime", _timings.scan_load_schema_ns);
    add_phase_timer("ScanCreate_MergeOptionsTime", _timings.scan_merge_options_ns);
    add_phase_timer("ScanCreate_LoadSnapshotTime", _timings.scan_load_snapshot_ns);
    add_phase_timer("ScanCreate_PathFactoryTime", _timings.scan_path_factory_ns);
    add_phase_timer("ScanCreate_ManifestCreateTime", _timings.scan_manifest_create_ns);
    add_phase_timer("ScanCreate_ManifestScanTime", _timings.scan_manifest_scan_ns);
    add_phase_timer("ScanCreate_ExecutorSetupTime", _timings.scan_executor_setup_ns);
    add_phase_timer("RowRangeIndexCreateTime", _timings.row_range_create_ns);
    add_phase_timer("PredicateEvalTime", _timings.predicate_eval_ns);
    add_phase_timer("ScoreExprEvalTime", _timings.score_expr_eval_ns);
    add_phase_timer("ResultAddOffsetTime", _timings.result_add_offset_ns);
    add_phase_timer("ResultSerializeTime", _timings.result_serialize_ns);

    if (!_paimon_fs->datacache_enabled()) {
        return;
    }

    const auto& stats = _paimon_fs->get_datacache_stats();
    COUNTER_UPDATE(profile->datacache_read_counter, stats.read_cache_count);
    COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_timer, stats.read_cache_ns);
    COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_counter, stats.write_cache_count);
    COUNTER_UPDATE(profile->datacache_write_bytes, stats.write_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_timer, stats.write_cache_ns);
    COUNTER_UPDATE(profile->datacache_write_fail_counter, stats.write_cache_fail_count);
    COUNTER_UPDATE(profile->datacache_write_fail_bytes, stats.write_cache_fail_bytes);
    COUNTER_UPDATE(profile->datacache_skip_write_counter, stats.skip_write_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_write_bytes, stats.skip_write_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_counter, stats.read_block_buffer_count);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);

    const auto sbs = _paimon_fs->get_shared_buffered_stats();
    COUNTER_UPDATE(profile->shared_buffered_shared_io_count, sbs.shared_io_count);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_bytes, sbs.shared_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_hit_io_count, sbs.hit_io_count);
    COUNTER_UPDATE(profile->shared_buffered_hit_io_bytes, sbs.hit_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_align_io_bytes, sbs.shared_align_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_timer, sbs.shared_io_timer);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_count, sbs.direct_io_count);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_bytes, sbs.direct_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_timer, sbs.direct_io_timer);
}

} // namespace starrocks
