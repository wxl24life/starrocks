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

#include "exec/paimon/paimon_scanner.h"

#include "fs/paimon/paimon_file_system.h"
#include "runtime/runtime_state.h"

namespace starrocks {

Status PaimonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status PaimonScanner::do_open(RuntimeState* runtime_state) {
    _reader = std::make_unique<PaimonNativeReader>(_scanner_params, _scanner_ctx, _cloud_conf, runtime_state,
                                                    &_fs_stats, &_app_stats);
    return _reader->open();
}

Status PaimonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(_reader->get_next(chunk));
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, (*chunk)->num_rows()));
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, (*chunk)->num_rows());
    return Status::OK();
}

void PaimonScanner::do_close(RuntimeState* runtime_state) noexcept {
    _reader.reset();
}

void PaimonScanner::do_update_counter(HdfsScanProfile* profile) {
    // Paimon C++ reader metrics
    auto metrics = _reader->get_reader_metrics();
    if (metrics) {
        RuntimeProfile* p = profile->runtime_profile;
        const std::string PAIMON_SECTION = "PaimonNativeReader";
        ADD_COUNTER(p, PAIMON_SECTION, TUnit::NONE);

        for (const auto& [key, value] : metrics->GetAllCounters()) {
            TUnit::type unit = TUnit::UNIT;
            int64_t counter_value = static_cast<int64_t>(value);
            if (key.find("bytes") != std::string::npos) {
                unit = TUnit::BYTES;
            } else if (key.find("latency") != std::string::npos) {
                unit = TUnit::TIME_NS;
                counter_value *= 1000;
            }
            auto* counter = ADD_CHILD_COUNTER(p, key, unit, PAIMON_SECTION);
            COUNTER_SET(counter, counter_value);
        }
    }

    // DataCache metrics
    auto paimon_fs = _reader->get_paimon_fs();
    if (paimon_fs->datacache_enabled()) {
        const auto& stats = paimon_fs->get_datacache_stats();
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

        const auto sbs = paimon_fs->get_shared_buffered_stats();
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
}

} // namespace starrocks
