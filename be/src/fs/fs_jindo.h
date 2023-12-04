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

#include "fs/fs.h"

#include "common/s3_uri.h"
#include "util/random.h"

#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_options.h"

namespace starrocks {

struct IsSpace: std::unary_function<int, bool> {
    bool operator()(int ch) const {
        return (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t');
    }
};

typedef std::unordered_map<std::string, std::string> HashMap;

class JindoSdkConfig {
public:
    JindoSdkConfig() = default;
    virtual ~JindoSdkConfig() = default;

    int loadConfig(const std::string& config);

    HashMap& get_configs();

private:
    // trim from start
    std::string lefttrim(const std::string& s);

    // trim from end
    std::string righttrim(const std::string& s);

    // trim from both ends
    std::string trim(const std::string &s);

private:
    std::vector<std::string> _text;
    HashMap _configs;
};

std::unique_ptr<FileSystem> new_fs_jindo(const FSOptions& options);

class JindoClientFactory {
public:
    static JindoClientFactory& instance() {
        static JindoClientFactory obj;
        return obj;
    }

    ~JindoClientFactory() = default;

    JindoClientFactory(const JindoClientFactory&) = delete;
    void operator=(const JindoClientFactory&) = delete;
    JindoClientFactory(JindoClientFactory&&) = delete;
    void operator=(JindoClientFactory&&) = delete;

    bool option_equals(const JdoOptions_t& left, const JdoOptions_t& right);
    StatusOr<std::string> get_local_user();
    StatusOr<JdoSystem_t> new_client(const S3URI& uri);

private:
    JindoClientFactory();

    static constexpr const char* OSS_ENDPOINT_KEY = "fs.oss.endpoint";
    static constexpr int MAX_CLIENTS_ITEMS = 8;

    std::mutex _lock;
    int _items{0};
    // _configs[i] is the client configuration of |_clients[i].
    JdoOptions_t _configs[MAX_CLIENTS_ITEMS];
    JdoSystem_t _clients[MAX_CLIENTS_ITEMS];
    Random _rand;
};

class JindoFileSystem : public FileSystem {
public:
    JindoFileSystem(const FSOptions& options) : _options(options) {}
    ~JindoFileSystem() override = default;

    JindoFileSystem(const JindoFileSystem&) = delete;
    void operator=(const JindoFileSystem&) = delete;
    JindoFileSystem(JindoFileSystem&&) = delete;
    void operator=(JindoFileSystem&&) = delete;

    Type type() const override { return OSS; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::new_sequential_file");
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::new_writable_file");
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::new_writable_file");
    }

    Status path_exists(const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::path_exists");
    }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("JindoFileSystem::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        return Status::NotSupported("JindoFileSystem::iterate_dir");
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        return Status::NotSupported("JindoFileSystem::iterate_dir2");
    }

    Status delete_file(const std::string& path) override { return Status::NotSupported("JindoFileSystem::delete_file"); }

    Status create_dir(const std::string& dirname) override {
        return Status::NotSupported("JindoFileSystem::create_dir");
    }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return Status::NotSupported("JindoFileSystem::create_dir_if_missing");
    }

    Status create_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("JindoFileSystem::create_dir_recursive");
    }

    Status delete_dir(const std::string& dirname) override {
        return Status::NotSupported("JindoFileSystem::delete_dir");
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("JindoFileSystem::delete_dir_recursive");
    }

    Status sync_dir(const std::string& dirname) override { return Status::NotSupported("JindoFileSystem::sync_dir"); }

    StatusOr<bool> is_directory(const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::is_directory");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("JindoFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("JindoFileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("JindoFileSystem::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("JindoFileSystem::link_file");
    }

private:
    FSOptions _options;
};

} // namespace starrocks
