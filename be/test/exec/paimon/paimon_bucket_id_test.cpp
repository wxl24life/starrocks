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

#include <fstream>
#include <sstream>

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "connector/utils.h"
#include "testutil/assert.h"

namespace starrocks {

static const std::string kTestDataDir = "./be/test/exec/paimon/test_data/";

class PaimonBucketIdTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    // Helper: build an Arrow RecordBatch from a StructArray and schema,
    // then call PaimonUtils::calculate_bucket_ids.
    StatusOr<Buffer<int32_t>> calculate_bucket_ids(const std::shared_ptr<arrow::Schema>& schema,
                                                        const std::shared_ptr<arrow::StructArray>& struct_array,
                                                        int32_t num_buckets, bool has_primary_key) {
        std::vector<std::string> bucket_keys;
        for (int i = 0; i < schema->num_fields(); i++) {
            bucket_keys.push_back(schema->field(i)->name());
        }
        auto record_batch = arrow::RecordBatch::Make(schema, struct_array->length(), struct_array->fields());
        return connector::PaimonUtils::calculate_bucket_ids(record_batch, bucket_keys, num_buckets, has_primary_key,
                                                            schema);
    }

    // Helper: read a test data file and return its content as a JSON array string.
    // The data file format: each line is a JSON array followed by ",\n".
    // We strip the trailing ",\n" and wrap in "[" and "]".
    static std::string read_data_file(const std::string& filename) {
        std::ifstream ifs(kTestDataDir + filename);
        EXPECT_TRUE(ifs.good()) << "Failed to open test data file: " << kTestDataDir + filename;
        std::stringstream ss;
        ss << ifs.rdbuf();
        std::string content = ss.str();
        // Strip trailing ",\n"
        if (content.size() >= 2) {
            content = content.substr(0, content.size() - 2);
        }
        return "[" + content + "]";
    }

    // Helper: run a Java-compatible test using a data file.
    // The data file contains rows where the last column is the expected bucket_id.
    // bucket_schema describes the data columns (excluding bucket_id).
    void run_java_compatible_test(const std::string& data_file, const std::shared_ptr<arrow::Schema>& bucket_schema,
                                  int32_t num_buckets, bool is_pk_table) {
        std::string content = read_data_file(data_file);

        // Parse with schema that includes the trailing bucket_id column
        arrow::FieldVector fields_with_id = bucket_schema->fields();
        fields_with_id.push_back(arrow::field("bucket_id", arrow::int32()));

        auto struct_array_with_id = std::dynamic_pointer_cast<arrow::StructArray>(
                arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_with_id), content).ValueOrDie());

        // Separate data columns from the expected bucket_id column
        arrow::ArrayVector data_arrays(struct_array_with_id->fields().begin(),
                                       struct_array_with_id->fields().end() - 1);
        auto data_struct_array =
                std::dynamic_pointer_cast<arrow::StructArray>(
                        arrow::StructArray::Make(data_arrays, bucket_schema->fields()).ValueOrDie());

        auto result = calculate_bucket_ids(bucket_schema, data_struct_array, num_buckets, is_pk_table);
        ASSERT_OK(result.status());

        // Compare with expected bucket IDs from the data file
        auto* expected_bucket_ids = arrow::internal::checked_cast<arrow::Int32Array*>(
                struct_array_with_id->field(bucket_schema->num_fields()).get());
        ASSERT_TRUE(expected_bucket_ids != nullptr);

        auto& actual = result.value();
        ASSERT_EQ(data_struct_array->length(), static_cast<int64_t>(actual.size()));
        for (int64_t i = 0; i < data_struct_array->length(); i++) {
            ASSERT_EQ(expected_bucket_ids->Value(i), actual[i]) << "Mismatch at row " << i;
        }
    }

    // Get local timezone name (same logic as paimon-cpp DateTimeUtils::GetLocalTimezoneName)
    static std::string get_local_timezone_name() {
        const char* tz = std::getenv("TZ");
        if (tz != nullptr && *tz != '\0') {
            return std::string(tz);
        }
        // Fallback: read /etc/localtime symlink
        char buf[256];
        ssize_t len = readlink("/etc/localtime", buf, sizeof(buf) - 1);
        if (len > 0) {
            buf[len] = '\0';
            std::string path(buf);
            // Extract timezone from path like /usr/share/zoneinfo/Asia/Shanghai
            auto pos = path.find("zoneinfo/");
            if (pos != std::string::npos) {
                return path.substr(pos + 9);
            }
        }
        return "UTC";
    }
};

// ==================== Tests aligned with paimon-cpp ====================

// Aligned with paimon-cpp TestVariantType: 4 rows, 12 columns of all basic types.
// Expected bucket IDs from Java: {11275, 12272, 6549, 11795}
TEST_F(PaimonBucketIdTest, TestVariantType) {
    arrow::FieldVector fields = {arrow::field("v0", arrow::boolean()),
                                 arrow::field("v1", arrow::int8()),
                                 arrow::field("v2", arrow::int16()),
                                 arrow::field("v3", arrow::int32()),
                                 arrow::field("v4", arrow::int64()),
                                 arrow::field("v5", arrow::float32()),
                                 arrow::field("v6", arrow::float64()),
                                 arrow::field("v7", arrow::date32()),
                                 arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("v9", arrow::decimal128(30, 20)),
                                 arrow::field("v10", arrow::utf8()),
                                 arrow::field("v11", arrow::binary())};
    auto schema = arrow::schema(fields);

    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), R"([
        [true, 10, 200, 65536, 123456789, 0.0, 0.0, 2000, -86399999999500, "2134.48690000000000000009", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", "Alice"],
        [false, -128, -32768, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308, -719528, -9223372036854775808, "-999999999999999999.99999999999999999999", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."],
        [true, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308, 2932896, 9223372036854775807, "999999999999999999.99999999999999999999", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."],
        [true, 0, 0, 0, 0, 1.4E-45, 4.9E-324, 0, 0, "0.00000000000000000000", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."]
    ])")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/12345, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    Buffer<int32_t> expected = {11275, 12272, 6549, 11795};
    ASSERT_EQ(expected, result.value());

    // Idempotency check (same as paimon-cpp)
    auto result2 = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/12345, /*has_primary_key=*/true);
    ASSERT_OK(result2.status());
    ASSERT_EQ(expected, result2.value());
}

// Aligned with paimon-cpp TestCompatibleWithJava: 5000 rows from data file,
// 12 data columns + 1 bucket_id column. Verifies hash compatibility with Java.
TEST_F(PaimonBucketIdTest, TestCompatibleWithJava) {
    auto bucket_schema = arrow::schema({arrow::field("v0", arrow::boolean()),
                                        arrow::field("v1", arrow::int8()),
                                        arrow::field("v2", arrow::int16()),
                                        arrow::field("v3", arrow::int32()),
                                        arrow::field("v4", arrow::int64()),
                                        arrow::field("v5", arrow::float32()),
                                        arrow::field("v6", arrow::float64()),
                                        arrow::field("v7", arrow::date32()),
                                        arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                        arrow::field("v9", arrow::decimal128(30, 20)),
                                        arrow::field("v10", arrow::utf8()),
                                        arrow::field("v11", arrow::binary())});

    run_java_compatible_test("record_for_bucket_id.data", bucket_schema,
                             /*num_buckets=*/12345, /*is_pk_table=*/true);
}

// Aligned with paimon-cpp TestCompatibleWithJavaWithNull: 5000 rows with nullable columns,
// 13 data columns (2 decimal columns with different precision) + 1 bucket_id column.
// First row is all null. Verifies null handling compatibility with Java.
TEST_F(PaimonBucketIdTest, TestCompatibleWithJavaWithNull) {
    auto bucket_schema = arrow::schema({arrow::field("v0", arrow::boolean()),
                                        arrow::field("v1", arrow::int8()),
                                        arrow::field("v2", arrow::int16()),
                                        arrow::field("v3", arrow::int32()),
                                        arrow::field("v4", arrow::int64()),
                                        arrow::field("v5", arrow::float32()),
                                        arrow::field("v6", arrow::float64()),
                                        arrow::field("v7", arrow::date32()),
                                        arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                        arrow::field("v9", arrow::decimal128(5, 2)),
                                        arrow::field("v10", arrow::decimal128(30, 2)),
                                        arrow::field("v11", arrow::utf8()),
                                        arrow::field("v12", arrow::binary())});

    run_java_compatible_test("record_with_null_for_bucket_id.data", bucket_schema,
                             /*num_buckets=*/12345, /*is_pk_table=*/false);
}

// Aligned with paimon-cpp TestCompatibleWithJavaWithTimestamp: 5000 rows with 8 timestamp columns
// of different precisions (sec/milli/micro/nano) with and without timezone.
TEST_F(PaimonBucketIdTest, TestCompatibleWithJavaWithTimestamp) {
    auto timezone = get_local_timezone_name();
    auto bucket_schema = arrow::schema({arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
                                        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
                                        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
                                        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
                                        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
                                        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
                                        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
                                        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone))});

    run_java_compatible_test("record_with_timestamp_for_bucket_id.data", bucket_schema,
                             /*num_buckets=*/12345, /*is_pk_table=*/false);
}

// Aligned with paimon-cpp TestUnawareBucket: DynamicBucketMode (num_buckets=-1),
// all bucket IDs should be 0.
TEST_F(PaimonBucketIdTest, TestUnawareBucket) {
    auto schema = arrow::schema({arrow::field("b0", arrow::int32())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), "[[10], [-1], [50]]")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/-1, /*has_primary_key=*/false);
    ASSERT_OK(result.status());

    Buffer<int32_t> expected = {0, 0, 0};
    ASSERT_EQ(expected, result.value());
}

// Aligned with paimon-cpp TestPostponeBucket: PostponeBucketMode (num_buckets=-2),
// all bucket IDs should be -2.
TEST_F(PaimonBucketIdTest, TestPostponeBucket) {
    auto schema = arrow::schema({arrow::field("b0", arrow::int32())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), "[[10], [-1], [50]]")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/-2, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    Buffer<int32_t> expected = {-2, -2, -2};
    ASSERT_EQ(expected, result.value());
}

// ==================== Additional StarRocks-specific tests ====================

// Basic hash property: same input value produces same bucket ID, results in range.
TEST_F(PaimonBucketIdTest, TestSingleIntColumn) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                      "[[100], [200], [300], [100]]")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/10, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    auto& bucket_ids = result.value();
    ASSERT_EQ(4, bucket_ids.size());
    ASSERT_EQ(bucket_ids[0], bucket_ids[3]);
    for (auto id : bucket_ids) {
        ASSERT_GE(id, 0);
        ASSERT_LT(id, 10);
    }
}

// String column hash consistency.
TEST_F(PaimonBucketIdTest, TestStringColumn) {
    auto schema = arrow::schema({arrow::field("name", arrow::utf8())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                      R"([["hello"], ["world"], ["hello"]])")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/5, /*has_primary_key=*/false);
    ASSERT_OK(result.status());

    auto& bucket_ids = result.value();
    ASSERT_EQ(3, bucket_ids.size());
    ASSERT_EQ(bucket_ids[0], bucket_ids[2]);
    for (auto id : bucket_ids) {
        ASSERT_GE(id, 0);
        ASSERT_LT(id, 5);
    }
}

// Multiple bucket key columns combination hash.
TEST_F(PaimonBucketIdTest, TestMultipleColumns) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                      R"([[1, "a"], [2, "b"], [1, "a"]])")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/100, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    auto& bucket_ids = result.value();
    ASSERT_EQ(3, bucket_ids.size());
    ASSERT_EQ(bucket_ids[0], bucket_ids[2]);
    for (auto id : bucket_ids) {
        ASSERT_GE(id, 0);
        ASSERT_LT(id, 100);
    }
}

// Single-row RecordBatch (simulates PaimonNativeWriter's Slice(0,1) optimization).
TEST_F(PaimonBucketIdTest, TestSingleRow) {
    auto schema = arrow::schema({arrow::field("v0", arrow::int32()), arrow::field("v1", arrow::utf8())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), R"([[42, "test"]])")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/10, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    auto& bucket_ids = result.value();
    ASSERT_EQ(1, bucket_ids.size());
    ASSERT_GE(bucket_ids[0], 0);
    ASSERT_LT(bucket_ids[0], 10);
}

// num_buckets=1: all bucket IDs should be 0.
TEST_F(PaimonBucketIdTest, TestSingleBucket) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), "[[1], [2], [3], [999]]")
                    .ValueOrDie());

    auto result = calculate_bucket_ids(schema, struct_array, /*num_buckets=*/1, /*has_primary_key=*/true);
    ASSERT_OK(result.status());

    for (auto id : result.value()) {
        ASSERT_EQ(0, id);
    }
}

} // namespace starrocks
