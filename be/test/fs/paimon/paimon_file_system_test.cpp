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

#include "fs/paimon/paimon_file_system.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class PaimonInputStreamTest : public testing::Test {
public:
    void SetUp() override {
        (void)fs::remove_all(kDir);
        ASSERT_TRUE(fs::create_directories(kDir).ok());

        // Non-owning shared_ptr around the process-global posix file system.
        _fs = std::shared_ptr<FileSystem>(FileSystem::Default(), [](FileSystem*) {});

        // Write a file whose byte at index i is deterministic: i % 251.
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto st_wfile = _fs->new_writable_file(opts, _path);
        ASSERT_TRUE(st_wfile.ok());
        auto wfile = std::move(st_wfile).value();
        std::string content;
        content.resize(kFileSize);
        for (int64_t i = 0; i < kFileSize; ++i) {
            content[i] = expected_byte(i);
        }
        ASSERT_TRUE(wfile->append(Slice(content)).ok());
        ASSERT_TRUE(wfile->close().ok());
    }

    void TearDown() override { (void)fs::remove_all(kDir); }

protected:
    static char expected_byte(int64_t abs_offset) { return static_cast<char>(abs_offset % 251); }

    std::unique_ptr<PaimonInputStream> open_stream() {
        // The file is created in SetUp(), so this open is expected to succeed; value()
        // aborts the test if it does not.
        auto st = _fs->new_random_access_file(_path);
        return std::make_unique<PaimonInputStream>(std::move(st).value(), _fs, _path);
    }

    static constexpr int64_t kFileSize = 1 << 20; // 1 MiB
    const std::string kDir = "./ut_dir/paimon_fs";
    std::string _path = "./ut_dir/paimon_fs/index_file";
    std::shared_ptr<FileSystem> _fs;
};

// A single async read returns the correct bytes.
TEST_F(PaimonInputStreamTest, ReadAsyncSingle) {
    auto stream = open_stream();
    const uint64_t offset = 4096;
    const uint32_t size = 8192;
    std::vector<char> buf(size, 0);

    std::atomic<bool> done{false};
    std::atomic<bool> ok{false};
    stream->ReadAsync(buf.data(), size, offset, [&](paimon::Status st) {
        ok.store(st.ok());
        done.store(true);
    });
    while (!done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(ok.load());
    for (uint32_t j = 0; j < size; ++j) {
        ASSERT_EQ(expected_byte(offset + j), buf[j]) << "mismatch at j=" << j;
    }
}

// The synchronous positional Read returns the correct bytes.
TEST_F(PaimonInputStreamTest, PositionalReadReturnsCorrectBytes) {
    auto stream = open_stream();
    const uint64_t offset = 123456;
    const uint32_t size = 4096;
    std::vector<char> buf(size, 0);

    auto result = stream->Read(buf.data(), size, offset);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(static_cast<int32_t>(size), result.value());
    for (uint32_t j = 0; j < size; ++j) {
        ASSERT_EQ(expected_byte(offset + j), buf[j]) << "mismatch at j=" << j;
    }
}

// Reading past EOF must fail rather than return partial/garbage data.
TEST_F(PaimonInputStreamTest, ReadAsyncPastEofFails) {
    auto stream = open_stream();
    const uint64_t offset = kFileSize - 16;
    const uint32_t size = 4096; // extends well past EOF
    std::vector<char> buf(size, 0);

    std::atomic<bool> done{false};
    std::atomic<bool> ok{true};
    stream->ReadAsync(buf.data(), size, offset, [&](paimon::Status st) {
        ok.store(st.ok());
        done.store(true);
    });
    while (!done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_FALSE(ok.load());
}

// Stress test: many threads issue many concurrent ReadAsync calls on ONE stream.
// The old implementation shared a stateful stream and would corrupt these reads.
TEST_F(PaimonInputStreamTest, ConcurrentReadAsyncIsThreadSafe) {
    auto stream = open_stream();

    constexpr int kThreads = 8;
    constexpr int kReadsPerThread = 256;
    constexpr int kTotal = kThreads * kReadsPerThread;
    constexpr uint32_t kReadSize = 3000; // not a power of two, to mix offsets

    std::vector<std::vector<char>> buffers(kTotal, std::vector<char>(kReadSize, 0));
    std::vector<uint64_t> offsets(kTotal);
    std::vector<uint8_t> ok_flags(kTotal, 0); // distinct bytes -> safe concurrent writes
    std::atomic<int> remaining{kTotal};

    for (int i = 0; i < kTotal; ++i) {
        offsets[i] = static_cast<uint64_t>((i * 7919) % (kFileSize - kReadSize));
    }

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int k = 0; k < kReadsPerThread; ++k) {
                const int i = t * kReadsPerThread + k;
                stream->ReadAsync(buffers[i].data(), kReadSize, offsets[i], [&, i](paimon::Status st) {
                    ok_flags[i] = st.ok() ? 1 : 0;
                    remaining.fetch_sub(1);
                });
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    while (remaining.load() != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (int i = 0; i < kTotal; ++i) {
        ASSERT_EQ(1, ok_flags[i]) << "read " << i << " failed";
        for (uint32_t j = 0; j < kReadSize; ++j) {
            ASSERT_EQ(expected_byte(offsets[i] + j), buffers[i][j])
                    << "corruption in read " << i << " at j=" << j;
        }
    }
}

} // namespace starrocks
