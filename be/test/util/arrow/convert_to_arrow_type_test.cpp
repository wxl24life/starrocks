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

#include <arrow/type.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "util/arrow/row_batch.h"

namespace starrocks {

class ConvertToArrowTypeTest : public testing::Test {
public:
    void SetUp() override { _original_value = config::enable_native_arrow_new_type; }

    void TearDown() override { config::enable_native_arrow_new_type = _original_value; }

private:
    bool _original_value;
};

TEST_F(ConvertToArrowTypeTest, test_date_to_utf8_by_default) {
    // Default behavior: DATE converts to utf8
    config::enable_native_arrow_new_type = false;

    TypeDescriptor type_desc(TYPE_DATE);
    std::shared_ptr<arrow::DataType> arrow_type;
    auto st = convert_to_arrow_type(type_desc, &arrow_type);

    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));
}

TEST_F(ConvertToArrowTypeTest, test_datetime_to_utf8_by_default) {
    // Default behavior: DATETIME converts to utf8
    config::enable_native_arrow_new_type = false;

    TypeDescriptor type_desc(TYPE_DATETIME);
    std::shared_ptr<arrow::DataType> arrow_type;
    auto st = convert_to_arrow_type(type_desc, &arrow_type);

    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));
}

TEST_F(ConvertToArrowTypeTest, test_date_to_date32_when_enabled) {
    // When enabled: DATE converts to Date32Type
    config::enable_native_arrow_new_type = true;

    TypeDescriptor type_desc(TYPE_DATE);
    std::shared_ptr<arrow::DataType> arrow_type;
    auto st = convert_to_arrow_type(type_desc, &arrow_type);

    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::date32()));
}

TEST_F(ConvertToArrowTypeTest, test_datetime_to_timestamp_when_enabled) {
    // When enabled: DATETIME converts to TimestampType with specified timezone
    config::enable_native_arrow_new_type = true;

    TypeDescriptor type_desc(TYPE_DATETIME);
    std::shared_ptr<arrow::DataType> arrow_type;
    std::string timezone = "Asia/Shanghai";
    auto st = convert_to_arrow_type(type_desc, &arrow_type, timezone);

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::TIMESTAMP);
    auto* ts_type = down_cast<const arrow::TimestampType*>(arrow_type.get());
    ASSERT_EQ(ts_type->unit(), arrow::TimeUnit::MICRO);
    ASSERT_EQ(ts_type->timezone(), timezone);
}

TEST_F(ConvertToArrowTypeTest, test_datetime_to_timestamp_with_default_timezone) {
    // When enabled with default timezone parameter
    config::enable_native_arrow_new_type = true;

    TypeDescriptor type_desc(TYPE_DATETIME);
    std::shared_ptr<arrow::DataType> arrow_type;
    auto st = convert_to_arrow_type(type_desc, &arrow_type);

    ASSERT_TRUE(st.ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::TIMESTAMP);
    auto* ts_type = down_cast<const arrow::TimestampType*>(arrow_type.get());
    ASSERT_EQ(ts_type->unit(), arrow::TimeUnit::MICRO);
    // Default timezone should be local timezone
    ASSERT_FALSE(ts_type->timezone().empty());
}

TEST_F(ConvertToArrowTypeTest, test_other_types_unchanged) {
    // Other types should not be affected by the config
    config::enable_native_arrow_new_type = false;

    // Test INT
    {
        TypeDescriptor type_desc(TYPE_INT);
        std::shared_ptr<arrow::DataType> arrow_type;
        auto st = convert_to_arrow_type(type_desc, &arrow_type);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(arrow_type->Equals(arrow::int32()));
    }

    // Test VARCHAR
    {
        TypeDescriptor type_desc(TYPE_VARCHAR);
        std::shared_ptr<arrow::DataType> arrow_type;
        auto st = convert_to_arrow_type(type_desc, &arrow_type);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));
    }

    // Test DOUBLE
    {
        TypeDescriptor type_desc(TYPE_DOUBLE);
        std::shared_ptr<arrow::DataType> arrow_type;
        auto st = convert_to_arrow_type(type_desc, &arrow_type);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(arrow_type->Equals(arrow::float64()));
    }
}

TEST_F(ConvertToArrowTypeTest, test_config_toggle) {
    TypeDescriptor date_type(TYPE_DATE);
    TypeDescriptor datetime_type(TYPE_DATETIME);
    std::shared_ptr<arrow::DataType> arrow_type;

    // First with config disabled
    config::enable_native_arrow_new_type = false;

    auto st = convert_to_arrow_type(date_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));

    st = convert_to_arrow_type(datetime_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));

    // Now enable the config
    config::enable_native_arrow_new_type = true;

    st = convert_to_arrow_type(date_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::date32()));

    st = convert_to_arrow_type(datetime_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::TIMESTAMP);

    // Disable again
    config::enable_native_arrow_new_type = false;

    st = convert_to_arrow_type(date_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));

    st = convert_to_arrow_type(datetime_type, &arrow_type);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(arrow_type->Equals(arrow::utf8()));
}

} // namespace starrocks
