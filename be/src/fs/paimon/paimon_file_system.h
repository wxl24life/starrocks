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

#include <mutex>

#include "cache/block_cache/cache_options.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"

namespace starrocks {

class Status;
class FileSystem;
class WritableFile;
class RandomAccessFile;
struct FSOptions;
struct HdfsScanStats;

class PaimonInputStream : public paimon::InputStream {
public:
    PaimonInputStream(std::unique_ptr<RandomAccessFile> file);
    ~PaimonInputStream() override;
    paimon::Status Close() override;
    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override;
    paimon::Result<int64_t> GetPos() const override;
    paimon::Result<int32_t> Read(char* buffer, uint32_t size) override;
    paimon::Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override;
    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(paimon::Status)>&& callback) override;
    paimon::Result<std::string> GetUri() const override;
    paimon::Result<uint64_t> Length() const override;

private:
    std::unique_ptr<RandomAccessFile> _file;
};

class PaimonOutputStream : public paimon::OutputStream {
public:
    PaimonOutputStream(std::unique_ptr<WritableFile> file);
    ~PaimonOutputStream() override;
    paimon::Status Close() override;
    paimon::Result<int32_t> Write(const char* buffer, uint32_t size) override;
    paimon::Status Flush() override;
    paimon::Result<int64_t> GetPos() const override;
    paimon::Result<std::string> GetUri() const override;

private:
    std::unique_ptr<WritableFile> _file;
};

class PaimonBasicFileStatus : public paimon::BasicFileStatus {
public:
    PaimonBasicFileStatus(const std::string& path, bool is_dir);
    ~PaimonBasicFileStatus() override;

    bool IsDir() const override;
    std::string GetPath() const override;

private:
    std::string path_;
    bool is_dir_;
};

class PaimonFileStatus : public paimon::FileStatus {
public:
    PaimonFileStatus(uint64_t len, int64_t last_modification_time, bool is_dir, std::string path);
    ~PaimonFileStatus() override;
    uint64_t GetLen() const override;
    bool IsDir() const override;
    std::string GetPath() const override;
    int64_t GetModificationTime() const override;

private:
    uint64_t _len;
    int64_t _last_mod_time;
    bool _is_dir;
    std::string _path;
};

class PaimonFileSystem : public paimon::FileSystem {
public:
    // `path` is the table root anchor used to resolve relative paths. `cloud_conf`
    // is forwarded from FE so JindoSDK / OSS clients sign requests with the right
    // credentials. `datacache_options` controls cache populate / priority / ttl.
    // `fs_stats` / `app_stats` (optional, may be nullptr for the write path) are
    // wired into CountedSeekableInputStream wrappers in Open() so that the
    // surrounding HdfsScanner machinery (and fe-audit scanBytes) sees actual IO
    // numbers — fs_stats tracks raw OSS bytes, app_stats tracks bytes the
    // paimon-cpp reader consumed post-cache.
    // Instances are constructed by `PaimonNativeReader::_create_paimon_fs` and
    // injected into paimon-cpp via `ReadContextBuilder::WithFileSystem`. The
    // global `PaimonFileSystemFactory` path is intentionally disabled — see
    // `PaimonFileSystemFactory::Create`.
    PaimonFileSystem(const std::string& path, const TCloudConfiguration& cloud_conf,
                     const DataCacheOptions& datacache_options, HdfsScanStats* fs_stats = nullptr,
                     HdfsScanStats* app_stats = nullptr);
    ~PaimonFileSystem() override;
    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path) const override;
    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path,
                                                              int64_t file_size) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override;
    paimon::Status Mkdirs(const std::string& path) const override;
    paimon::Status Rename(const std::string& src, const std::string& dst) const override;
    paimon::Status Delete(const std::string& path, bool recursive) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string& path) const override;
    paimon::Status ListDir(const std::string& dir,
                           std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const override;
    paimon::Status ListFileStatus(const std::string& path,
                                  std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const override;
    paimon::Result<bool> Exists(const std::string& path) const override;

    bool datacache_enabled() const { return _enable_datacache; }

    io::CacheInputStream::Stats get_datacache_stats() const;

    // Aggregate stats across all SharedBufferedInputStreams held by this FS,
    // taken under _streams_mutex so concurrent prefetch Open() can't race the
    // vector with the reader of these stats.
    struct SharedBufferedStats {
        int64_t shared_io_count = 0;
        int64_t shared_io_bytes = 0;
        int64_t hit_io_count = 0;
        int64_t hit_io_bytes = 0;
        int64_t shared_align_io_bytes = 0;
        int64_t shared_io_timer = 0;
        int64_t direct_io_count = 0;
        int64_t direct_io_bytes = 0;
        int64_t direct_io_timer = 0;
    };
    SharedBufferedStats get_shared_buffered_stats() const;

private:
    Status delete_internal(const std::string& path, bool is_dir, bool recursive) const;

    TCloudConfiguration _cloud_configuration;
    std::unique_ptr<starrocks::FileSystem> _fs;
    bool _enable_datacache = false;
    DataCacheOptions _datacache_options{};
    HdfsScanStats* _fs_stats = nullptr;
    HdfsScanStats* _app_stats = nullptr;

    mutable std::mutex _streams_mutex;
    mutable std::vector<std::shared_ptr<io::CacheInputStream>> _cache_streams;
    mutable std::vector<std::shared_ptr<io::SharedBufferedInputStream>> _shared_buffered_streams;
};

class PaimonFileSystemFactory : public paimon::FileSystemFactory {
public:
    static const char IDENTIFIER[];
    const char* Identifier() const override;
    paimon::Result<std::unique_ptr<paimon::FileSystem>> Create(
            const std::string& path, const std::map<std::string, std::string>& options) const override;
};

class PaimonOptions {
public:
    // The value of ROOT_PATH should be referred to
    // https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/CoreOptions.java#L158
    static const std::string ROOT_PATH;
};

} // namespace starrocks
