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

#pragma once

#include <gen_cpp/CloudConfiguration_types.h>
#include <paimon/global_index/global_index_result.h>

#include "exec/hdfs_scanner.h"

namespace starrocks {

class PaimonFileSystem;

class PaimonGlobalIndexScanner final : public HdfsScanner {
public:
    explicit PaimonGlobalIndexScanner(const TCloudConfiguration& cloud_conf) : _cloud_conf(cloud_conf) {}
    ~PaimonGlobalIndexScanner() override = default;
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

private:
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> evaluateGlobalIndex();
    int32_t return_rows = 0;

    // Cloud credentials forwarded from THdfsScanNode.cloud_configuration. Stored as a copy so
    // that ::evaluateGlobalIndex (called from do_get_next, after the FE-side stack has gone)
    // can still read the OSS keys when constructing the paimon::FileSystem.
    // (HdfsScanStats _fs_stats / _app_stats are inherited from HdfsScanner; we pass pointers to
    // those base members into PaimonFileSystem so the base class's profile machinery — which
    // reads them via num_bytes_read() / update_hdfs_counter() — picks up our IO numbers.)
    TCloudConfiguration _cloud_conf;

    // Held so do_update_counter() can pull get_datacache_stats() / get_shared_buffered_stats()
    // after the scan finishes.
    std::shared_ptr<PaimonFileSystem> _paimon_fs;

    // Per-phase wall-clock timings accumulated inside evaluateGlobalIndex() and do_get_next().
    // Profile surfaces them as TIME_NS counters under a "PaimonGlobalIndex" parent section,
    // so a slow scan can be attributed to JSON parse / FS build / index meta open / predicate
    // eval / ANN topN eval / result serialize without diving into BE logs.
    struct PhaseTimings {
        int64_t evaluate_total_ns = 0;
        int64_t json_parse_ns = 0;
        int64_t fs_build_ns = 0;
        int64_t scan_create_ns = 0;
        int64_t row_range_create_ns = 0;
        int64_t predicate_eval_ns = 0;
        int64_t score_expr_eval_ns = 0;
        int64_t result_add_offset_ns = 0;
        int64_t result_serialize_ns = 0;
    };
    PhaseTimings _timings;

    // Captured for the single structured INFO log line emitted at end of evaluateGlobalIndex();
    // intentionally NOT in the profile (identity / per-shard context, not aggregable).
    int32_t _log_readers_created = 0;
    int64_t _log_result_rows = 0;
};

} // namespace starrocks
