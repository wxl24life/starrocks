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

#include "paimon_file_system.h"

#include <algorithm>
#include <cstdint>
#include <iostream>

#include "common/config.h"
#include "exec/counted_seekable_input_stream.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "util/monotime.h"
#include "util/threadpool.h"

namespace starrocks {
namespace {

// Dedicated, process-lifetime thread pool that runs PaimonInputStream::ReadAsync tasks.
// Lazily built on first use; intentionally leaked so it is never torn down at static
// destruction time (which could race with other process singletons). Returns nullptr
// if the pool could not be created -- callers then fall back to an inline read.
ThreadPool* get_paimon_async_read_pool() {
    static ThreadPool* pool = []() -> ThreadPool* {
        std::unique_ptr<ThreadPool> built;
        Status st = ThreadPoolBuilder("paimon_aio")
                            .set_min_threads(0)
                            .set_max_threads(std::max(1, config::paimon_async_read_thread_pool_size))
                            .set_max_queue_size(INT32_MAX)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                            .build(&built);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to create paimon async read thread pool, reason: " << st.message();
            return nullptr;
        }
        LOG(INFO) << "Created paimon async read thread pool, max_threads="
                  << std::max(1, config::paimon_async_read_thread_pool_size);
        return built.release();
    }();
    return pool;
}

// Perform a positional read by opening an independent, short-lived RandomAccessFile.
// Because the file owns its own offset and prefetch buffer, any number of these calls
// may run concurrently without locking. `file_size`, when >= 0, is passed through so
// the underlying file system can skip its own size lookup (e.g. an S3 HeadObject).
paimon::Status read_fully_with_fresh_stream(const std::shared_ptr<starrocks::FileSystem>& fs, const std::string& path,
                                            int64_t file_size, char* buffer, uint32_t size, uint64_t offset) {
    if (fs == nullptr) {
        LOG(WARNING) << "paimon positioned read has no file system, file=" << path;
        return paimon::Status::IOError(fmt::format("file '{}' has no file system for positioned read", path));
    }
    FileInfo file_info;
    file_info.path = path;
    if (file_size >= 0) {
        file_info.size = file_size;
    }
    auto rf = fs->new_random_access_file(RandomAccessFileOptions(), file_info);
    if (!rf.ok()) {
        LOG(WARNING) << "paimon positioned read failed to open file=" << path
                     << ", reason: " << rf.status().detailed_message();
        return paimon::Status::IOError(fmt::format("Failed to open file {} for positioned read, reason: {}", path,
                                                   rf.status().detailed_message()));
    }
    auto status = (*rf)->read_at_fully(offset, buffer, size);
    if (!status.ok()) {
        LOG(WARNING) << "paimon positioned read failed, file=" << path << ", offset=" << offset
                     << ", request_size=" << size << ", reason: " << status.detailed_message();
        return paimon::Status::IOError(
                fmt::format("Failed to read file {}, reason: {}", path, status.detailed_message()));
    }
    return paimon::Status::OK();
}

} // namespace

const char PaimonFileSystemFactory::IDENTIFIER[] = "paimon";
const std::string PaimonOptions::ROOT_PATH = "path";

const char* PaimonFileSystemFactory::Identifier() const {
    return IDENTIFIER;
}

paimon::Result<std::unique_ptr<paimon::FileSystem>> PaimonFileSystemFactory::Create(
        const std::string& /*path*/, const std::map<std::string, std::string>& /*options*/) const {
    // All real call sites construct PaimonFileSystem explicitly with the FE-supplied
    // TCloudConfiguration and inject it via ReadContextBuilder::WithFileSystem(), so
    // this factory fallback should never be reached. The previous implementation
    // hard-coded TCloudType::ALIYUN, which would silently sign requests with the
    // wrong credentials on non-OSS backends. Surface the contract violation here so
    // the offending paimon-cpp code path is visible in logs rather than hiding
    // behind a confusing "AccessDenied" downstream error.
    return paimon::Status::IOError(
            "PaimonFileSystemFactory::Create is not supported; PaimonFileSystem must be "
            "constructed explicitly and injected via WithFileSystem().");
}

REGISTER_PAIMON_FACTORY(PaimonFileSystemFactory);

uint64_t PaimonFileStatus::GetLen() const {
    return _len;
}

std::string PaimonFileStatus::GetPath() const {
    return _path;
}

bool PaimonFileStatus::IsDir() const {
    return _is_dir;
}

int64_t PaimonFileStatus::GetModificationTime() const {
    return _last_mod_time;
}

PaimonBasicFileStatus::PaimonBasicFileStatus(const std::string& path, bool is_dir)
        : path_(std::move(path)), is_dir_(is_dir) {}

PaimonBasicFileStatus::~PaimonBasicFileStatus() = default;

std::string PaimonBasicFileStatus::GetPath() const {
    return path_;
}

bool PaimonBasicFileStatus::IsDir() const {
    return is_dir_;
}

PaimonFileStatus::PaimonFileStatus(uint64_t len, int64_t last_modification_time, bool is_dir, std::string path)
        : _len(len), _last_mod_time(last_modification_time), _is_dir(is_dir), _path(std::move(path)) {}

PaimonFileStatus::~PaimonFileStatus() = default;

PaimonFileSystem::PaimonFileSystem(const std::string& path, const TCloudConfiguration& cloud_conf,
                                   const DataCacheOptions& datacache_options, HdfsScanStats* fs_stats,
                                   HdfsScanStats* app_stats)
        : _cloud_configuration(cloud_conf),
          _enable_datacache(datacache_options.enable_datacache),
          _datacache_options(datacache_options),
          _external_fs_stats(fs_stats),
          _external_app_stats(app_stats) {
    if (fs_stats != nullptr) {
        _owned_fs_stats = std::make_shared<HdfsScanStats>();
    }
    if (app_stats != nullptr) {
        _owned_app_stats = std::make_shared<HdfsScanStats>();
    }
    FSOptions fs_options(&_cloud_configuration);
    auto st = starrocks::FileSystem::CreateUniqueFromString(path, fs_options);
    if (!st.ok()) {
        // It looks like no scenario can reach this code path, but just in case.
        LOG(ERROR) << "Failed to create delegate file system, reason: " << st.status().detailed_message();
    }
    _fs = std::move(st).value();
}

PaimonFileSystem::~PaimonFileSystem() {
    // Sync the owned counters wrapped into CountedSeekableInputStream back to the
    // external HdfsScanScanner stats so the scanner profile sees the IO this
    // PaimonFileSystem performed. Only `io_ns / io_count / bytes_read` are touched
    // by CountedSeekableInputStream, so only those need to be merged; the rest of
    // HdfsScanStats is parquet/orc-specific and stays zero on the ANN path.
    //
    // PaimonFileSystem is owned by PaimonGlobalIndexScanner via `_paimon_fs`
    // shared_ptr; the scanner destructor releases that shared_ptr before its
    // inherited `HdfsScanScanner::_fs_stats / _app_stats` value members go out
    // of scope, so the external pointers here are guaranteed alive.
    if (_external_fs_stats != nullptr && _owned_fs_stats != nullptr) {
        _external_fs_stats->io_ns += _owned_fs_stats->io_ns;
        _external_fs_stats->io_count += _owned_fs_stats->io_count;
        _external_fs_stats->bytes_read += _owned_fs_stats->bytes_read;
    }
    if (_external_app_stats != nullptr && _owned_app_stats != nullptr) {
        _external_app_stats->io_ns += _owned_app_stats->io_ns;
        _external_app_stats->io_count += _owned_app_stats->io_count;
        _external_app_stats->bytes_read += _owned_app_stats->bytes_read;
    }
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> PaimonFileSystem::Create(const std::string& path,
                                                                               bool overwrite) const {
    VLOG(10) << "Creating path " << path;
    WritableFileOptions options;
    options.mode = starrocks::FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    auto st = _fs->new_writable_file(options, path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to create file {}, reason: {}", path, st.status().detailed_message()));
    }
    return std::make_unique<PaimonOutputStream>(std::move(st).value());
}

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path) const {
    return Open(path, -1);
}

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path,
                                                                           int64_t file_size) const {
    VLOG(10) << "Open path " << path << " file_size=" << file_size;
    RandomAccessFileOptions options;
    auto st = _fs->new_random_access_file(options, path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to open file {}, reason: {}", path, st.status().detailed_message()));
    }
    auto raw_file = std::move(st).value();
    const std::string& filename = raw_file->filename();

    // Wrap the raw fs stream so every actual storage IO (network OSS read,
    // post-cache disk read) lands in `_owned_fs_stats`. Layered below
    // SharedBufferedInputStream / CacheInputStream so cache hits don't count
    // towards FS-level IO. Mirrors HdfsScanner::create_random_access_file.
    // Pass the owned shared_ptr (not the external raw ptr) so the stats survive
    // every queued paimon_aio / paimon-cpp lumina async IO regardless of
    // HiveDataSource teardown order; the destructor syncs the accumulated
    // counters back to the external HdfsScanner stats.
    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();
    if (_owned_fs_stats != nullptr) {
        input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, _owned_fs_stats);
    }

    // [R12.8 backstop] Honor process-wide `config::datacache_enable` even when the
    // FE-supplied `_datacache_options.enable_datacache` is false. Without this, any FE
    // path that constructs PaimonFileSystem without wiring datacache_options (e.g. a
    // future caller missing the plumbing) would silently fall through to a raw,
    // uncached stream and we wouldn't notice until a regression in OSS traffic shows
    // up. When the backstop kicks in we also force populate=true so the cache actually
    // fills on cold reads; per-query options (priority/ttl/io_adaptor/async_populate)
    // fall back to their sensible defaults.
    const bool use_cache = _enable_datacache || config::datacache_enable;
    const bool backstop_only = !_enable_datacache && config::datacache_enable;

    if (!use_cache) {
        auto counted_file = std::make_unique<RandomAccessFile>(input_stream, filename);
        if (file_size >= 0) {
            counted_file->set_size(file_size);
        }
        return std::make_unique<PaimonInputStream>(std::move(counted_file), _fs, path,
                                                    /*cache_enabled=*/false);
    }

    // file_size < 0 sentinel: caller doesn't know the size, fetch via HEAD.
    if (file_size < 0) {
        auto size_st = raw_file->get_size();
        if (!size_st.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get file size for {}, reason: {}", path,
                                                       size_st.status().detailed_message()));
        }
        file_size = size_st.value();
    }

    auto shared_buffered_input_stream =
            std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions coalesce_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(coalesce_options);

    // modification_time=0 is safe for Paimon since data files are immutable
    auto cache_input_stream =
            std::make_shared<io::CacheInputStream>(shared_buffered_input_stream, filename, file_size, 0);
    // When the backstop is the only thing enabling cache, force populate=true so cache
    // actually fills. Otherwise honor per-query options as before.
    cache_input_stream->set_enable_populate_cache(backstop_only ? true
                                                                : _datacache_options.enable_populate_datacache);
    cache_input_stream->set_enable_async_populate_mode(_datacache_options.enable_datacache_async_populate_mode);
    cache_input_stream->set_enable_cache_io_adaptor(_datacache_options.enable_datacache_io_adaptor);
    cache_input_stream->set_priority(_datacache_options.datacache_priority);
    cache_input_stream->set_ttl_seconds(_datacache_options.datacache_ttl_seconds);
    cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
    shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());

    {
        std::lock_guard<std::mutex> lock(_streams_mutex);
        _cache_streams.push_back(cache_input_stream);
        _shared_buffered_streams.push_back(shared_buffered_input_stream);
    }

    // Wrap the final post-cache stream so paimon-cpp's reader-visible reads
    // (cache hit + cache miss alike) land in `_owned_app_stats`. Must be the
    // outermost wrapper so its io_ns reflects total time spent in IO. Owned
    // shared_ptr passed for async-safety per the FS-level rationale above.
    std::shared_ptr<io::SeekableInputStream> final_stream = cache_input_stream;
    if (_owned_app_stats != nullptr) {
        final_stream = std::make_shared<CountedSeekableInputStream>(final_stream, _owned_app_stats);
    }

    auto cache_file = std::make_unique<RandomAccessFile>(final_stream, filename);
    cache_file->set_size(file_size);
    return std::make_unique<PaimonInputStream>(std::move(cache_file), _fs, path,
                                                /*cache_enabled=*/true);
}

paimon::Status PaimonFileSystem::Delete(const std::string& path, bool recursive) const {
    VLOG(10) << "Deleting path " << path << ", recursive: " << recursive;
    auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check whether {} is directory or not from delete, reason: {}", path,
                            st.status().detailed_message()));
    }
    Status status = delete_internal(path, st.value(), recursive);
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to delete path {}, reason: {}", path, status.detailed_message()));
    }
    return paimon::Status::OK();
}

Status PaimonFileSystem::delete_internal(const std::string& path, bool is_dir, bool recursive) const {
    VLOG(10) << "Deleting path " << path << ", is dir: " << is_dir << ", recursive: " << recursive;
    if (is_dir) {
        if (recursive) {
            return _fs->delete_dir_recursive(path);
        }
        return _fs->delete_dir(path);
    }
    return _fs->delete_file(path);
}

paimon::Result<bool> PaimonFileSystem::Exists(const std::string& path) const {
    VLOG(10) << "Checking path " << path << " exists or not.";
    auto st = _fs->path_exists(path);
    if (st.ok()) {
        return true;
    }
    if (st.is_not_found()) {
        return false;
    }
    return paimon::Status::IOError(
            fmt::format("Error occurs while checking path {} exist, reason: {}", path, st.detailed_message()));
}

paimon::Status PaimonFileSystem::Mkdirs(const std::string& path) const {
    VLOG(10) << "Creating directory " << path;
    auto st = _fs->create_dir_recursive(path);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to mkdirs for {}, reason: {}", path, st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::Rename(const std::string& src, const std::string& dst) const {
    VLOG(10) << "Renaming path " << src << " to " << dst;
    auto st = _fs->is_directory(src);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check whether source path {} is directory or not from rename, reason: {}", src,
                            st.status().detailed_message()));
    }
    // todo: should we asure src is exists while processing?
    if (st.value()) {
        // src is directory
        return paimon::Status::IOError("Not support rename directory currently.");
    }
    auto status = _fs->rename_file(src, dst);
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to rename source {} to dst {}, reason: {}", src, dst, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> PaimonFileSystem::GetFileStatus(const std::string& path) const {
    VLOG(10) << "Get path status for " << path;
    if (const auto st = _fs->path_exists(path); !st.ok()) {
        if (st.is_not_found()) {
            return paimon::Status::NotExist(fmt::format("Path {} is not exist.", path));
        }
        return paimon::Status::IOError(
                fmt::format("Get file status for {} failed, reason: {}", path, st.detailed_message()));
    }
    auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format(
                "Get file status but failed to check whether path {} is directory or not from get status, reason: {}",
                path, st.status().detailed_message()));
    }
    auto st1 = _fs->get_file_size(path);
    if (!st1.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to get file size for {}, reason: {}", path, st1.status().detailed_message()));
    }
    auto st2 = _fs->get_file_modified_time(path);
    if (!st2.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file modified time for {}, reason: {}", path,
                                                   st2.status().detailed_message()));
    }
    return std::make_unique<PaimonFileStatus>(st1.value(), st2.value(), st.value(), path);
}

paimon::Status PaimonFileSystem::ListDir(
        const std::string& dir, std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const {
    if (dir.empty()) {
        return paimon::Status::IOError("dir is empty.");
    }
    VLOG(10) << "List Dir status for " << dir;
    const auto st = _fs->is_directory(dir);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to check {} is directory or not from list dir, reason: {}",
                                                   dir, st.status().detailed_message()));
    }
    if (!st.value()) {
        return paimon::Status::IOError(fmt::format("Cannot get status for {}, because it is not a directory.", dir));
    }

    if (file_status_list == nullptr) {
        file_status_list = new std::vector<std::unique_ptr<paimon::BasicFileStatus>>();
    }
    const auto status = _fs->iterate_dir2(dir, [this, &file_status_list](const DirEntry& dir_entry) -> bool {
        const std::string filename(dir_entry.name.begin(), dir_entry.name.end());
        if (filename.size() != dir_entry.name.size()) {
            // that means file name contains delimiter character which will cause a failure cast.
            return false;
        }
        file_status_list->emplace_back(
                std::make_unique<PaimonBasicFileStatus>(filename, dir_entry.is_dir.value_or(false)));
        return true;
    });
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to get status for {}, reason: {}", dir, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::ListFileStatus(
        const std::string& path, std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const {
    if (path.empty()) {
        return paimon::Status::IOError("path is empty.");
    }
    VLOG(10) << "List file status for " << path;
    const auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check {} is directory or not from list status, reason: {}", path,
                            st.status().detailed_message()));
    }

    if (file_status_list == nullptr) {
        file_status_list = new std::vector<std::unique_ptr<paimon::FileStatus>>();
    }
    if (st.value()) {
        // path is directory
        const auto status = _fs->iterate_dir2(path, [this, &file_status_list](const DirEntry& dir_entry) -> bool {
            const std::string filename(dir_entry.name.begin(), dir_entry.name.end());
            if (filename.size() != dir_entry.name.size()) {
                // that means file name contains delimiter character which will cause a failure cast.
                return false;
            }
            auto res = std::make_unique<PaimonFileStatus>(dir_entry.size.value_or(0), dir_entry.mtime.value_or(0),
                                                          dir_entry.is_dir.value_or(true), filename);
            file_status_list->emplace_back(std::move(res));
            return true;
        });
        if (!status.ok()) {
            return paimon::Status::IOError(
                    fmt::format("Failed to list file status for {}, reason: {}", path, status.detailed_message()));
        }
    } else {
        auto res = GetFileStatus(path);
        if (!res.ok() && !res.status().IsNotExist()) {
            return paimon::Status::IOError(fmt::format("Failed to get file status for {}, reason: {}", path,
                                                       res.status().detail()->ToString()));
        }
        file_status_list->emplace_back(std::move(res).value());
    }
    return paimon::Status::OK();
}

PaimonInputStream::PaimonInputStream(std::unique_ptr<RandomAccessFile> file, std::shared_ptr<starrocks::FileSystem> fs,
                                     std::string path, bool cache_enabled)
        : _file(std::move(file)),
          _fs(std::move(fs)),
          _path(std::move(path)),
          _cache_enabled(cache_enabled) {}

PaimonInputStream::~PaimonInputStream() = default;

int64_t PaimonInputStream::ensure_file_size() {
    std::call_once(_size_once, [this]() {
        auto st = _file->get_size();
        if (st.ok()) {
            _cached_size = static_cast<int64_t>(st.value());
        } else {
            LOG(WARNING) << "paimon failed to resolve file size, file=" << _path
                         << ", reason: " << st.status().detailed_message();
            _cached_size = -1;
        }
    });
    return _cached_size;
}

paimon::Result<int32_t> PaimonInputStream::Read(char* buffer, uint32_t size) {
    auto st = _file->read(buffer, size);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to read file {}, reason: {}", _file->filename(), st.status().detailed_message()));
    }
    return static_cast<int32_t>(st.value());
}

paimon::Status PaimonInputStream::Close() {
    return paimon::Status::OK();
}

paimon::Result<uint64_t> PaimonInputStream::Length() const {
    auto st = _file->get_size();
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get length for file {}, reason: {}", _file->filename(),
                                                   st.status().detailed_message()));
    }
    return static_cast<uint64_t>(st.value());
}

paimon::Result<int32_t> PaimonInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    // [R12.8 fix] When `_file` is wrapped with CacheInputStream, route positional reads
    // through it via read_at_fully() so they hit DataCache. read_at_fully does not
    // mutate the cursor (so concurrent sequential Read(buf,size) callers see no
    // surprise), and `_positional_mutex` serializes any racing positional callers to
    // protect the SharedBufferedInputStream prefetch buffer + CacheInputStream internal
    // state underneath. Cache-disabled path (or rollback flag) falls back to opening a
    // fresh raw stream per call -- the legacy behavior, which keeps positional reads
    // lock-free but bypasses DataCache entirely.
    if (_cache_enabled && config::paimon_cached_positional_read_enable) {
        std::lock_guard<std::mutex> lock(_positional_mutex);
        auto st = _file->read_at_fully(offset, buffer, size);
        if (!st.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to read file {} at offset {}, reason: {}",
                                                       _file->filename(), offset, st.detailed_message()));
        }
        return static_cast<int32_t>(size);
    }
    // Cache disabled (or emergency rollback): independent stream per call, no cache.
    const int64_t file_size = ensure_file_size();
    paimon::Status status = read_fully_with_fresh_stream(_fs, _path, file_size, buffer, size, offset);
    if (!status.ok()) {
        return status;
    }
    return static_cast<int32_t>(size);
}

paimon::Status PaimonInputStream::Seek(int64_t offset, paimon::SeekOrigin origin) {
    int64_t new_pos = offset;
    if (origin == paimon::SeekOrigin::FS_SEEK_CUR) {
        /* set file offset to current plus offset */
        auto res = GetPos();
        if (!res.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}",
                                                       _file->filename(), res.status().ToString()));
        }
        new_pos = res.value() + offset;
    } else if (origin == paimon::SeekOrigin::FS_SEEK_END) {
        /* set file offset to EOF plus offset */
        auto res = Length();
        if (!res.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get length for file {}, reason: {}",
                                                       _file->filename(), res.status().ToString()));
        }
        new_pos = res.value() + offset;
    }
    auto st = _file->seek(new_pos);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to seek file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<int64_t> PaimonInputStream::GetPos() const {
    auto st = _file->position();
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}", _file->filename(),
                                                   st.status().detailed_message()));
    }
    return st.value();
}

paimon::Result<std::string> PaimonInputStream::GetUri() const {
    return _file->filename();
}

void PaimonInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                  std::function<void(paimon::Status)>&& callback) {
    // Cached `_file` path: route through the cached RandomAccessFile so positional
    // reads hit DataCache + reuse the open JindoSDK chain. Run inline on the caller
    // thread — paimon-cpp's LuminaFileReader::ReadAsync is already invoked from
    // lumina's own SimpleThreadPool worker, so the StarRocks scan thread sees no
    // additional blocking, and `_file->read_at_fully` is a fast in-memory copy
    // backed by DataCache. Running inline also avoids capturing `this` into a
    // queued task, which would re-introduce the heap-use-after-free that
    // CountedSeekableInputStream::read_at_fully hit when the owning HiveDataSource
    // finished before the queued ReadAsync task could run. The two remaining
    // lifetime invariants the cached read relies on:
    //   * CountedSeekableInputStream holds a shared_ptr<HdfsScanStats> keepalive
    //     so `_stats` survives HiveDataSource teardown,
    //   * JindoFileSystem deep-copies `cloud_configuration` so downstream
    //     JindoClient ops don't dereference freed FSOptions.
    // are established below by PaimonFileSystem / JindoFileSystem construction.
    if (_cache_enabled && config::paimon_cached_positional_read_enable) {
        std::lock_guard<std::mutex> lock(_positional_mutex);
        auto st = _file->read_at_fully(offset, buffer, size);
        if (!st.ok()) {
            callback(paimon::Status::IOError(fmt::format("Failed to read file {} at offset {}, reason: {}",
                                                         _file->filename(), offset, st.detailed_message())));
            return;
        }
        callback(paimon::Status::OK());
        return;
    }

    // Legacy fresh-stream async path: capture `fs` (shared_ptr<FileSystem>) and `path`
    // by value, open an independent RandomAccessFile per call inside the worker. Safe
    // by construction (JindoFileSystem deep-copies cloud_configuration). Higher CPU
    // because every call builds + frees the full JindoSDK chain.
    const int64_t file_size = ensure_file_size();
    std::function<void()> task = [fs = _fs, path = _path, file_size, buffer, size, offset,
                                  cb = std::move(callback)]() {
        cb(read_fully_with_fresh_stream(fs, path, file_size, buffer, size, offset));
    };
    ThreadPool* pool = get_paimon_async_read_pool();
    Status submit_st =
            pool != nullptr ? pool->submit_func(task) : Status::InternalError("paimon async read pool unavailable");
    if (!submit_st.ok()) {
        // Pool unavailable or queue rejected the task: run it inline so the callback
        // is still invoked exactly once and is never dropped.
        LOG(WARNING) << "paimon async read falls back to inline execution, file=" << _path << ", offset=" << offset
                     << ", reason: " << submit_st.message();
        task();
    }
}

PaimonOutputStream::PaimonOutputStream(std::unique_ptr<WritableFile> file) : _file(std::move(file)) {}

PaimonOutputStream::~PaimonOutputStream() = default;

paimon::Result<int32_t> PaimonOutputStream::Write(const char* buffer, uint32_t size) {
    if (const auto st = _file->append(Slice(buffer, size)); !st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to write file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Result(static_cast<int32_t>(size));
}

paimon::Status PaimonOutputStream::Close() {
    const auto st = _file->close();
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to close file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonOutputStream::Flush() {
    WritableFile::FlushMode mode = WritableFile::FLUSH_SYNC;
    auto st = _file->flush(mode);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to flush file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<int64_t> PaimonOutputStream::GetPos() const {
    return _file->size();
}

paimon::Result<std::string> PaimonOutputStream::GetUri() const {
    return _file->filename();
}

io::CacheInputStream::Stats PaimonFileSystem::get_datacache_stats() const {
    io::CacheInputStream::Stats total;
    std::lock_guard<std::mutex> lock(_streams_mutex);
    for (const auto& stream : _cache_streams) {
        const auto& s = stream->stats();
        total.read_cache_ns += s.read_cache_ns;
        total.write_cache_ns += s.write_cache_ns;
        total.read_cache_count += s.read_cache_count;
        total.write_cache_count += s.write_cache_count;
        total.write_mem_cache_bytes += s.write_mem_cache_bytes;
        total.write_disk_cache_bytes += s.write_disk_cache_bytes;
        total.read_cache_bytes += s.read_cache_bytes;
        total.read_mem_cache_bytes += s.read_mem_cache_bytes;
        total.read_disk_cache_bytes += s.read_disk_cache_bytes;
        total.write_cache_bytes += s.write_cache_bytes;
        total.skip_read_cache_count += s.skip_read_cache_count;
        total.skip_read_cache_bytes += s.skip_read_cache_bytes;
        total.skip_write_cache_count += s.skip_write_cache_count;
        total.skip_write_cache_bytes += s.skip_write_cache_bytes;
        total.write_cache_fail_count += s.write_cache_fail_count;
        total.write_cache_fail_bytes += s.write_cache_fail_bytes;
        total.read_block_buffer_bytes += s.read_block_buffer_bytes;
        total.read_block_buffer_count += s.read_block_buffer_count;
    }
    return total;
}

PaimonFileSystem::SharedBufferedStats PaimonFileSystem::get_shared_buffered_stats() const {
    SharedBufferedStats total;
    std::lock_guard<std::mutex> lock(_streams_mutex);
    for (const auto& stream : _shared_buffered_streams) {
        total.shared_io_count += stream->shared_io_count();
        total.shared_io_bytes += stream->shared_io_bytes();
        total.hit_io_count += stream->hit_io_count();
        total.hit_io_bytes += stream->hit_io_bytes();
        total.shared_align_io_bytes += stream->shared_align_io_bytes();
        total.shared_io_timer += stream->shared_io_timer();
        total.direct_io_count += stream->direct_io_count();
        total.direct_io_bytes += stream->direct_io_bytes();
        total.direct_io_timer += stream->direct_io_timer();
    }
    return total;
}

} // namespace starrocks
