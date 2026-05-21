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

#include <memory>
#include <vector>

#include <exec/arrow_to_starrocks_converter.h>
#include <exec/hdfs_scanner.h>
#include <gen_cpp/CloudConfiguration_types.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/reader/batch_reader.h>

namespace starrocks {

class PaimonFileSystem;

class PaimonNativeReader {
public:
    PaimonNativeReader(const HdfsScannerParams& scanner_params, const HdfsScannerContext& scanner_ctx,
                       const TCloudConfiguration& cloud_conf, RuntimeState* state, HdfsScanStats* fs_stats,
                       HdfsScanStats* app_stats);
    ~PaimonNativeReader() = default;

    Status open();
    Status get_next(ChunkPtr* chunk);

    std::shared_ptr<paimon::Metrics> get_reader_metrics() const {
        return _reader ? _reader->GetReaderMetrics() : nullptr;
    }

    std::shared_ptr<PaimonFileSystem> get_paimon_fs() const { return _paimon_fs; }

private:
    std::shared_ptr<PaimonFileSystem> _create_paimon_fs();
    Status _next_batch();
    Status _append_batch_to_read_chunk();
    Status _fill_dst_chunk(ChunkPtr* dst);
    void _init_read_fields();

    bool _chunk_is_full() const;
    bool _batch_is_exhausted() const;

    const HdfsScannerParams& _scanner_params;
    const HdfsScannerContext& _scanner_ctx;
    const TCloudConfiguration& _cloud_conf;
    HdfsScanStats* _fs_stats;
    HdfsScanStats* _app_stats;
    std::shared_ptr<paimon::MemoryPool> _tracked_pool;
    int _max_chunk_size;
    int _max_batch_rows = 10000;

    // Match the pre-refactor PaimonScanner read loop:
    //   * paimon-cpp produces batches of size _max_batch_rows
    //   * we slice each paimon batch into _max_chunk_size-sized output chunks via these
    //     two indices, accumulating across multiple paimon batches when needed.
    int64_t _batch_start_idx = 0;
    int64_t _chunk_start_idx = 0;
    bool _scanner_eof = false;

    std::shared_ptr<PaimonFileSystem> _paimon_fs;
    std::unique_ptr<paimon::BatchReader> _reader;
    ObjectPool _pool;
    std::vector<std::string> _field_names;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;
    std::vector<Expr*> _cast_exprs;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
    // Internal staging buffer. Pre-allocated in open() (Columns + conv_funcs +
    // cast_exprs are built once), then reset/filled per get_next() call. At the
    // end of each call, _fill_dst_chunk swaps the data into the caller's
    // freshly-cloned chunk, leaving _read_chunk's Columns empty for the next
    // reset(). Arrow type is derived from slot type via convert_to_arrow_type;
    // safe because paimon-cpp applies schema evolution before emitting batches,
    // so every arrow batch conforms to the slot-derived type.
    ChunkPtr _read_chunk;
};

} // namespace starrocks
