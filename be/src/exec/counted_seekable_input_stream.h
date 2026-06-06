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

#include "exec/hdfs_scanner.h"
#include "io/seekable_input_stream.h"
#include "util/raw_container.h"

namespace starrocks {

// Wraps a SeekableInputStream and accumulates read bytes / io count / io ns
// into the supplied HdfsScanStats. Used by the HDFS scanner stack to track
// both FS-level IO (wrapping the raw fs stream) and app-level IO (wrapping
// the final post-cache stream).
class CountedSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    // Existing API: borrow a HdfsScanStats* whose lifetime is managed externally.
    // Safe only when the caller can guarantee the stats outlive every IO on this
    // stream -- i.e. the synchronous scan path where HdfsScanner stays alive for
    // the whole read sequence.
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream, HdfsScanStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

    // Async-safe API: the stream takes shared ownership of the stats. Required
    // for any caller whose stream may outlive the original stats owner -- e.g.
    // PaimonFileSystem wrapping a cached `_file` whose ReadAsync lambdas are run
    // on paimon_aio / paimon-cpp lumina worker threads after HdfsScanner (which
    // by-value owns the original HdfsScanStats) has been torn down with
    // HiveDataSource. The keepalive shared_ptr ensures `_stats` stays valid for
    // every queued IO regardless of the upstream teardown order.
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream,
                                         std::shared_ptr<HdfsScanStats> stats_keepalive)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(stream),
              _stats_keepalive(std::move(stats_keepalive)),
              _stats(_stats_keepalive.get()) {}

    ~CountedSeekableInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read(data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        _stats->bytes_read += size;
        return _stream->read_at_fully(offset, data, size);
    }

    StatusOr<std::string_view> peek(int64_t count) override { return _stream->peek(count); }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read_at(offset, out, count));
        _stats->bytes_read += nread;
        return nread;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    // Holds the stats alive for the duration of this stream when the caller went
    // through the async-safe constructor. nullptr for the legacy raw-ptr path,
    // in which case `_stats` aliases caller-managed memory.
    std::shared_ptr<HdfsScanStats> _stats_keepalive;
    HdfsScanStats* _stats;
};

} // namespace starrocks
