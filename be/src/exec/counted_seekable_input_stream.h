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
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream, HdfsScanStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

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
    HdfsScanStats* _stats;
};

} // namespace starrocks
