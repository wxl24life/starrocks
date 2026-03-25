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

#include "storage/lake/lake_replication_txn_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>

#include "agent/master_info.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/failpoint/fail_point.h"
#ifdef USE_STAROS
#include <fslib/file.h>
#include <fslib/file_system.h>
#include <fslib/stat.h>
#endif
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "fs/fs_starlet.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "service/staros_worker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::lake {

// UT for shared data cross-cluster replication
class SharedDataReplicationTxnManagerTest : public testing::TestWithParam<KeysType> {
public:
    SharedDataReplicationTxnManagerTest() { _test_dir = kTestDirectory; }

    ~SharedDataReplicationTxnManagerTest() override = default;

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

        _src_tablet_metadata = generate_tablet_metadata(GetParam());
        _target_tablet_metadata = generate_tablet_metadata(GetParam());

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_src_tablet_metadata));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));

        _src_tablet_id = _src_tablet_metadata->id();
        _target_tablet_id = _target_tablet_metadata->id();
        // target visible version
        _version = _target_tablet_metadata->version();

        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(
                "table_schema_service_disable_remote_schema_for_load");
        if (fp != nullptr) {
            fp->setMode(trigger_mode);
        }
    }

    void TearDown() override {
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(
                "table_schema_service_disable_remote_schema_for_load");
        if (fp != nullptr) {
            fp->setMode(trigger_mode);
        }

#ifdef USE_STAROS
        if (config::starlet_cache_dir.compare(0, 5, std::string("/tmp/")) == 0) {
            // Clean cache directory
            std::string cmd = fmt::format("rm -rf {}", config::starlet_cache_dir);
            ::system(cmd.c_str());
        }
#endif

        config::enable_transparent_data_encryption = false;

        // check primary index cache's ref
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // check trash files already removed
        for (const auto& file : _trash_files) {
            EXPECT_FALSE(fs::path_exist(file));
        }
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    std::shared_ptr<TabletMetadataPB> generate_tablet_metadata(KeysType keys_type) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_cumulative_point(0);
        metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = metadata->mutable_schema();
        schema->set_keys_type(keys_type);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
        }
        return metadata;
    }

    Chunk generate_data(int64_t chunk_size, int shift, int update_ratio) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<int> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * update_ratio;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));

        for (int i = 0; i < chunk_size; i++) {
            v2[i] = v0[i] * 4;
        }
        auto c2 = Int32Column::create();
        c2->append_numbers(v2.data(), v2.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
    }

    void write_src_tablet_data() {
        auto chunk0 = generate_data(kChunkSize, 0, 3);
        auto chunk1 = generate_data(kChunkSize, 0, 3);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            indexes[i] = i;
        }

        auto version = 1;
        // normal write
        for (int i = 0; i < 3; i++) {
            auto txn_id = next_id();
            ASSIGN_OR_ABORT(auto delta_writer, lake::DeltaWriterBuilder()
                                                       .set_tablet_manager(_tablet_mgr.get())
                                                       .set_tablet_id(_src_tablet_id)
                                                       .set_txn_id(txn_id)
                                                       .set_partition_id(_src_partition_id)
                                                       .set_mem_tracker(_mem_tracker.get())
                                                       .set_schema_id(_src_tablet_metadata->schema().id())
                                                       .build());
            ASSERT_OK(delta_writer->open());
            ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            // Publish version
            auto txn_info = TxnInfoPB();
            txn_info.set_txn_id(txn_id);
            txn_info.set_combined_txn_log(false);
            txn_info.set_commit_time(0);
            auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
            ASSERT_OK(lake::publish_version(_tablet_mgr.get(), _src_tablet_id, version, version + 1, txn_info_span));
            version++;
        }
        ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(_src_tablet_id, version));
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
        EXPECT_EQ(new_tablet_metadata->version(), 4);
        // src visible version
        _src_version = new_tablet_metadata->version();
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_replication";
    constexpr static int kChunkSize = 12;

    std::unique_ptr<TabletManager> _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;

    int64_t _src_tablet_id = 10000;
    int64_t _target_tablet_id = 20000;

    std::shared_ptr<TabletMetadata> _src_tablet_metadata;
    std::shared_ptr<TabletMetadata> _target_tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::vector<std::string> _trash_files;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;

    std::string _test_dir;

    int64_t _transaction_id = 300;
    int64_t _table_id = 30001;
    int64_t _partition_id = 30002;
    int64_t _version = 1;
    int64_t _src_version = 1;
    int32_t _schema_hash = 368169781;
    int64_t _virtual_tablet_id = 40001;
    int64_t _src_db_id = 40002;
    int64_t _src_table_id = 40003;
    int64_t _src_partition_id = 40004;
};

TEST_P(SharedDataReplicationTxnManagerTest, test_replicate_no_missing_versions) {
    TReplicateSnapshotRequest request;
    request.__set_transaction_id(_transaction_id);
    request.__set_table_id(_table_id);
    request.__set_partition_id(_partition_id);
    request.__set_tablet_id(_target_tablet_id);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    request.__set_schema_hash(_schema_hash);
    request.__set_visible_version(_version);
    request.__set_data_version(_version);
    // src tablet
    request.__set_src_tablet_id(_src_tablet_id);
    request.__set_src_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    request.__set_src_visible_version(_version); // same as `data_version`
    request.__set_src_db_id(_src_db_id);
    request.__set_src_table_id(_src_tablet_id);
    request.__set_src_partition_id(_src_partition_id);

    // virtual tablet
    request.__set_virtual_tablet_id(_virtual_tablet_id);

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_FALSE(status.ok());
}

// Tests for LakeReplicationTxnManager::copy_non_segment_file_with_retry
class CopyNonSegmentFileWithRetryTest : public testing::Test {
protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(_test_dir));
        SyncPoint::GetInstance()->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
        (void)fs::remove_all(_test_dir);
    }

    // Create a test file with specified content
    Status create_test_file(const std::string& path, const std::string& content) {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(path));
        ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(opts, path));
        RETURN_IF_ERROR(wf->append(content));
        return wf->close();
    }

    static constexpr const char* kTestDirectory = "test_non_segment_copy_retry";
    std::string _test_dir = kTestDirectory;
};

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_error_retry_succeeds) {
    // Create a source file
    std::string src_path = lake::join_path(_test_dir, "test.sst");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.sst");
    std::string content(4096, 'A');
    ASSERT_OK(create_test_file(src_path, content));

    // Inject error on first copy attempt via TEST_ERROR_POINT, succeed on subsequent attempts
    int call_count = 0;
    SyncPoint::GetInstance()->SetCallBack("fs::copy_file", [&](void* arg) {
        auto* st = static_cast<Status*>(arg);
        if (call_count++ == 0) {
            *st = Status::IOError("Injected transient copy error");
        }
    });

    // Simulate the retry logic from lake_replication_txn_manager.cpp
    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
    ASSIGN_OR_ABORT(auto expected_size, src_fs->get_file_size(src_path));
    EXPECT_EQ(expected_size, content.size());

    const size_t buff_size = std::max<size_t>(std::min<size_t>(expected_size, 16 * 1024 * 1024), 1 * 1024 * 1024);
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    int max_retry = 3;
    Status copy_status;
    size_t final_file_size = 0;
    for (int retry = 0; retry < max_retry; ++retry) {
        auto res = fs::copy_file(src_path, src_fs, dst_path, nullptr, opts, buff_size);
        if (!res.ok()) {
            copy_status = res.status();
            continue;
        }
        final_file_size = *res;
        if (static_cast<int64_t>(final_file_size) == static_cast<int64_t>(expected_size)) {
            copy_status = Status::OK();
            break;
        }
        copy_status = Status::Corruption(fmt::format("File size mismatch after copy: expected={}, actual={}, src={}",
                                                     expected_size, final_file_size, src_path));
    }

    // First attempt fails, second succeeds
    ASSERT_OK(copy_status);
    EXPECT_EQ(final_file_size, content.size());
    EXPECT_EQ(call_count, 2); // 1 failed + 1 successful
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_size_mismatch_exhausts_retries) {
    // Create a source file
    std::string src_path = lake::join_path(_test_dir, "test.delvec");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.delvec");
    std::string content(8192, 'B');
    ASSERT_OK(create_test_file(src_path, content));

    // Make every copy return a truncated size via the sync point callback
    SyncPoint::GetInstance()->SetCallBack("lake_replication_non_segment_copy_size", [&](void* arg) {
        auto* size = static_cast<size_t*>(arg);
        // Simulate truncation: report half the actual size
        *size = *size / 2;
    });

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
    ASSIGN_OR_ABORT(auto expected_size, src_fs->get_file_size(src_path));

    const size_t buff_size = std::max<size_t>(std::min<size_t>(expected_size, 16 * 1024 * 1024), 1 * 1024 * 1024);
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    int max_retry = std::max(1, config::lake_replication_max_file_copy_retry);
    Status copy_status;
    size_t final_file_size = 0;
    int retry_count = 0;
    for (int retry = 0; retry < max_retry; ++retry) {
        auto res = fs::copy_file(src_path, src_fs, dst_path, nullptr, opts, buff_size);
        if (!res.ok()) {
            copy_status = res.status();
            continue;
        }
        final_file_size = *res;
        TEST_SYNC_POINT_CALLBACK("lake_replication_non_segment_copy_size", &final_file_size);
        if (static_cast<int64_t>(final_file_size) == static_cast<int64_t>(expected_size)) {
            copy_status = Status::OK();
            break;
        }
        copy_status = Status::Corruption(fmt::format("File size mismatch after copy: expected={}, actual={}, src={}",
                                                     expected_size, final_file_size, src_path));
        retry_count++;
    }

    // All retries should be exhausted with size mismatch
    EXPECT_FALSE(copy_status.ok());
    EXPECT_TRUE(copy_status.is_corruption()) << copy_status;
    EXPECT_NE(std::string::npos, copy_status.message().find("File size mismatch after copy"));
    EXPECT_EQ(retry_count, max_retry);
}

#ifdef USE_STAROS
TEST(LakeReplicationTxnManagerTest, test_convert_s3_path_to_starlet_uri) {
    // Test case from user: convert S3 path to starlet URI
    std::string s3_path =
            "s3://cdp-hangzhou/cdp-hangzhou/5/186d104c-7078-4d21-ae3f-087873046b97/db135540/135542/135541/meta/"
            "0000000000021178_0000000000000002.meta";
    int64_t shard_id = 12345;

    std::string expected_uri =
            "staros://12345/cdp-hangzhou/5/186d104c-7078-4d21-ae3f-087873046b97/db135540/135542/135541/meta/"
            "0000000000021178_0000000000000002.meta";
    std::string actual_uri = lake::convert_s3_path_to_starlet_uri(s3_path, shard_id);

    EXPECT_EQ(expected_uri, actual_uri);
}

TEST(LakeReplicationTxnManagerTest, test_convert_s3_path_to_starlet_uri_edge_cases) {
    // Test various edge cases
    int64_t shard_id = 10086;

    // Normal S3 path: s3://bucket/path -> staros://shard_id/path
    EXPECT_EQ("staros://10086/path/file.txt",
              lake::convert_s3_path_to_starlet_uri("s3://bucket/path/file.txt", shard_id));

    // S3 path with nested bucket-like structure
    EXPECT_EQ("staros://10086/folder/file.txt",
              lake::convert_s3_path_to_starlet_uri("s3://bucket/folder/file.txt", shard_id));

    // S3 path with only bucket, no path after
    EXPECT_EQ("staros://10086", lake::convert_s3_path_to_starlet_uri("s3://bucket", shard_id));

    // S3 path with bucket and trailing slash
    EXPECT_EQ("staros://10086", lake::convert_s3_path_to_starlet_uri("s3://bucket/", shard_id));

    // Invalid S3 path (not starting with s3://) - should still convert but log warning
    EXPECT_EQ("staros://10086/invalid/path", lake::convert_s3_path_to_starlet_uri("invalid/path", shard_id));

    // Empty path after s3://
    EXPECT_EQ("staros://10086", lake::convert_s3_path_to_starlet_uri("s3://", shard_id));
}

class TryBuildSourceTabletMetaWithFallbackTest : public testing::Test {
public:
    TryBuildSourceTabletMetaWithFallbackTest() = default;
    ~TryBuildSourceTabletMetaWithFallbackTest() override = default;

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(_test_dir));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(_src_tablet_id);
        _tablet_metadata->set_version(_version);
        _tablet_metadata->set_next_rowset_id(1);
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto fs_or = FileSystem::CreateSharedFromString(_test_dir);
        CHECK(fs_or.ok());
        _shared_fs = fs_or.value();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    Status create_metadata_at_path(const std::string& meta_dir) {
        RETURN_IF_ERROR(fs::create_directories(meta_dir));
        auto filename = lake::tablet_metadata_filename(_src_tablet_id, _version);
        auto filepath = lake::join_path(meta_dir, filename);
        return _tablet_mgr->put_tablet_metadata(_tablet_metadata, filepath);
    }

    std::string build_current_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("db{}/{}/{}/meta", _src_db_id, _src_table_id, _src_partition_id));
    }
    std::string build_current_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("db{}/{}/{}/data", _src_db_id, _src_table_id, _src_partition_id));
    }

    std::string build_legacy1_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/{}/meta", _src_table_id, _src_partition_id));
    }
    std::string build_legacy1_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/{}/data", _src_table_id, _src_partition_id));
    }

    std::string build_legacy2_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/meta", _src_table_id));
    }
    std::string build_legacy2_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/data", _src_table_id));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_fallback_meta";

    std::unique_ptr<TabletManager> _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<FileSystem> _shared_fs;

    std::string _test_dir = kTestDirectory;
    int64_t _src_tablet_id = 63457;
    int64_t _src_db_id = 56764;
    int64_t _src_table_id = 56970;
    int64_t _src_partition_id = 63453;
    int64_t _version = 2;
    TTransactionId _txn_id = 12345;
};

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_current_format_success) {
    std::string meta_dir = build_current_format_meta_dir();
    std::string data_dir = build_current_format_data_dir();
    ASSERT_OK(create_metadata_at_path(meta_dir));

    std::string test_meta_dir = meta_dir;
    std::string test_data_dir = data_dir;
    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(meta_dir, test_meta_dir);
    EXPECT_EQ(data_dir, test_data_dir);
    EXPECT_EQ(_src_tablet_id, result.value()->id());
    EXPECT_EQ(_version, result.value()->version());
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_fallback_to_legacy1_format_success) {
    std::string legacy1_meta_dir = build_legacy1_format_meta_dir();
    std::string legacy1_data_dir = build_legacy1_format_data_dir();
    ASSERT_OK(create_metadata_at_path(legacy1_meta_dir));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(legacy1_meta_dir, test_meta_dir);
    EXPECT_EQ(legacy1_data_dir, test_data_dir);
    EXPECT_EQ(_src_tablet_id, result.value()->id());
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_fallback_to_legacy2_format_success) {
    std::string legacy2_meta_dir = build_legacy2_format_meta_dir();
    std::string legacy2_data_dir = build_legacy2_format_data_dir();
    ASSERT_OK(create_metadata_at_path(legacy2_meta_dir));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(legacy2_meta_dir, test_meta_dir);
    EXPECT_EQ(legacy2_data_dir, test_data_dir);
    EXPECT_EQ(_src_tablet_id, result.value()->id());
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_all_attempts_fail_not_found) {
    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found()) << result.status();
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_path_transformation_correctness) {
    std::string legacy2_meta_dir = build_legacy2_format_meta_dir();
    ASSERT_OK(create_metadata_at_path(legacy2_meta_dir));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    EXPECT_NE(std::string::npos, test_meta_dir.find(fmt::format("db{}", _src_db_id)));
    EXPECT_NE(std::string::npos, test_meta_dir.find(std::to_string(_src_partition_id)));

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();

    EXPECT_EQ(std::string::npos, test_meta_dir.find(fmt::format("db{}", _src_db_id)));
    EXPECT_EQ(std::string::npos, test_meta_dir.find(std::to_string(_src_partition_id)));
    EXPECT_EQ(std::string::npos, test_data_dir.find(fmt::format("db{}", _src_db_id)));
    EXPECT_EQ(std::string::npos, test_data_dir.find(std::to_string(_src_partition_id)));
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_priority_current_over_legacy) {
    ASSERT_OK(create_metadata_at_path(build_current_format_meta_dir()));
    ASSERT_OK(create_metadata_at_path(build_legacy1_format_meta_dir()));
    ASSERT_OK(create_metadata_at_path(build_legacy2_format_meta_dir()));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();
    std::string original_meta_dir = test_meta_dir;

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(original_meta_dir, test_meta_dir);
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_priority_legacy1_over_legacy2) {
    ASSERT_OK(create_metadata_at_path(build_legacy1_format_meta_dir()));
    ASSERT_OK(create_metadata_at_path(build_legacy2_format_meta_dir()));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(build_legacy1_format_meta_dir(), test_meta_dir);
    EXPECT_EQ(build_legacy1_format_data_dir(), test_data_dir);
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_different_version) {
    int64_t different_version = 100;

    _tablet_metadata->set_version(different_version);
    std::string legacy1_meta_dir = build_legacy1_format_meta_dir();
    ASSERT_OK(fs::create_directories(legacy1_meta_dir));
    auto filename = lake::tablet_metadata_filename(_src_tablet_id, different_version);
    auto filepath = lake::join_path(legacy1_meta_dir, filename);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(_tablet_metadata, filepath));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, different_version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(different_version, result.value()->version());
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_data_dir_consistency) {
    ASSERT_OK(create_metadata_at_path(build_legacy2_format_meta_dir()));

    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();

    std::string expected_base = lake::join_path(_test_dir, std::to_string(_src_table_id));
    EXPECT_EQ(expected_base + "/meta", test_meta_dir);
    EXPECT_EQ(expected_base + "/data", test_data_dir);
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_db_id_not_in_path) {
    std::string custom_meta_dir = lake::join_path(_test_dir, "custom/path/meta");
    std::string custom_data_dir = lake::join_path(_test_dir, "custom/path/data");

    std::string legacy2_meta_dir = lake::join_path(_test_dir, "custom/meta");
    ASSERT_OK(create_metadata_at_path(legacy2_meta_dir));

    std::string test_meta_dir = custom_meta_dir;
    std::string test_data_dir = custom_data_dir;

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    ASSERT_TRUE(result.ok()) << result.status();
}

// Test Case 4: has_full_path=false, new_fs_starlet returns valid fs, meta build fails
TEST_F(LakeReplicationRemoteStorageTest, test_no_full_path_meta_build_failure) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    // SyncPoint makes new_fs_starlet return a valid (but mock) filesystem
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // The filesystem creation succeeds (not nullptr), but reading tablet metadata
    // via the mock filesystem will fail. The error should NOT be
    // "Failed to create virtual starlet filesystem".
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"))
            << "Should have passed the nullptr check, error: " << status;
}
// Test Case 5: has_full_path=true with non-S3 path should fail with InvalidArgument
TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_non_s3_type_rejected) {
    auto request = build_request(false /* with_full_path */);
    // Manually set a non-S3 full path (e.g., HDFS path)
    request.__set_src_partition_full_path("hdfs://namenode/path/to/data");

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Full path must be S3 type"));
}

// Test Case 6: Fast cancel - when min_active_txn_id > txn_id, replication should abort
// before copying any files.
TEST_F(LakeReplicationRemoteStorageTest, test_fast_cancel_txn_aborted_before_copy) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing segment files.
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segments("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    rowset->add_segments("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Save original master info and set min_active_txn_id > txn_id to trigger fast cancel
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(_transaction_id + 1);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_aborted()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Lake replication cancelled, transaction is aborted"));
}

// Test Case 7: Fast cancel - when min_active_txn_id advances during file copy (between
// iterations), replication should abort after copying some files but before all files.
TEST_F(LakeReplicationRemoteStorageTest, test_fast_cancel_txn_aborted_during_copy) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing segment files
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segments("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    rowset->add_segments("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Save original master info. Start with min_active_txn_id <= txn_id (no abort yet).
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    // In the before_copy SyncPoint callback, advance min_active_txn_id past txn_id
    // after the first file copy iteration. The next iteration's fast cancel check
    // will detect the abort.
    int before_copy_count = 0;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) {
                                              before_copy_count++;
                                              if (before_copy_count == 1) {
                                                  TMasterInfo updated_info = get_master_info();
                                                  updated_info.__set_min_active_txn_id(_transaction_id + 1);
                                                  (void)update_master_info(updated_info);
                                              }
                                          });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    // The first iteration's before_copy callback runs (file copy attempt happens but may
    // fail due to mock filesystem). Either:
    // a) The first file copy fails (IOError from mock fs) - this is acceptable, OR
    // b) If we somehow get past the first copy, the second iteration detects the abort.
    EXPECT_FALSE(status.ok());
    EXPECT_GE(before_copy_count, 1) << "SyncPoint before_copy should have been invoked at least once";
}

// Test Case 8: No fast cancel - when min_active_txn_id <= txn_id, the fast cancel check
// should NOT abort the replication (it should proceed to the file copy step).
TEST_F(LakeReplicationRemoteStorageTest, test_no_fast_cancel_when_txn_active) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing a segment file
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segments("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Set min_active_txn_id <= txn_id so fast cancel does NOT trigger
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(_transaction_id); // equal, not greater
    ASSERT_TRUE(update_master_info(info));

    bool before_copy_invoked = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) { before_copy_invoked = true; });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    // The fast cancel check should NOT have triggered. The function should proceed past
    // the fast cancel check to the before_copy SyncPoint and then to file copy.
    // The file copy will fail due to the mock filesystem, but that's expected.
    EXPECT_TRUE(before_copy_invoked) << "before_copy SyncPoint should have been reached (fast cancel did not trigger)";
    EXPECT_FALSE(status.is_aborted()) << "Should not abort when min_active_txn_id <= txn_id, status: " << status;
}
#endif // USE_STAROS

INSTANTIATE_TEST_SUITE_P(SharedDataReplicationTxnManagerTest, SharedDataReplicationTxnManagerTest,
                         testing::Values(KeysType::DUP_KEYS, KeysType::AGG_KEYS, KeysType::PRIMARY_KEYS));

} // namespace starrocks::lake