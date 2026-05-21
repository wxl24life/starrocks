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

// This test verifies that convert_to_arrow_type(slot_type) produces arrow types
// that are compatible with the arrow-to-starrocks conversion pipeline.
// It ensures that building converters from slot_desc->type() (instead of from
// actual arrow batch schema) is safe for paimon native reader.
//
// The helper mirrors the current PaimonNativeReader flow exactly:
//   1. convert_to_arrow_type(slot_type) to derive arrow type          (_init_column_converters)
//   2. ParquetScanner::new_column() to build conv_func + cast_expr    (_init_column_converters)
//   3. Reuse new_column()'s returned raw column                       (_init_read_chunk)
//   4. ParquetScanner::convert_array_to_column() with ArrowConvertContext  (_append_batch_to_chunk)
//   5. cast_expr->evaluate_checked() to produce final column          (_finalize_chunk)

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/parquet_scanner.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/arrow/row_batch.h"

namespace starrocks {

class ArrowConverterRoundtripTest : public ::testing::Test {
public:
    void SetUp() override { date::init_date_cache(); }
};

// Mirrors the current PaimonNativeReader flow end-to-end.
// conv_ctx->state is needed for timestamp timezone rectification.
static StatusOr<ColumnPtr> convert_and_verify(const TypeDescriptor& type_desc,
                                              const std::shared_ptr<arrow::Array>& array, size_t expected_rows,
                                              RuntimeState* state = nullptr,
                                              const std::string& timezone = "Asia/Shanghai") {
    ObjectPool pool;
    ConvertFuncTree conv_func;
    Expr* cast_expr = nullptr;
    ColumnPtr raw_column;

    // Step 1-2: derive arrow type from slot type and build converter (mirrors _init_column_converters)
    std::shared_ptr<arrow::DataType> arrow_type;
    RETURN_IF_ERROR(convert_to_arrow_type(type_desc, &arrow_type, timezone));
    auto* slot_desc = pool.add(new SlotDescriptor(0, "test_col", type_desc));
    RETURN_IF_ERROR(ParquetScanner::new_column(arrow_type.get(), slot_desc, &raw_column, &conv_func,
                                               &cast_expr, pool, true));

    // Step 3: reuse new_column()'s returned raw column. This matches the current
    // PaimonNativeReader implementation after storing raw columns in _raw_columns.
    raw_column->reserve(expected_rows);
    auto read_chunk = std::make_shared<Chunk>();
    read_chunk->append_column(raw_column, slot_desc->id());

    // Step 4: convert via convert_array_to_column with real context (mirrors _append_batch_to_chunk)
    ArrowConvertContext conv_ctx;
    conv_ctx.current_slot = slot_desc;
    conv_ctx.state = state;
    Filter filter(expected_rows, 1);
    RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(&conv_func, expected_rows, array.get(), raw_column, 0, 0,
                                                            &filter, &conv_ctx));

    // Apply filter (mirrors _finalize_chunk)
    read_chunk->filter(filter);

    // Step 5: apply cast_expr to produce final column (mirrors _finalize_chunk)
    ASSIGN_OR_RETURN(auto final_column, cast_expr->evaluate_checked(nullptr, read_chunk.get()));
    final_column = ColumnHelper::unfold_const_column(type_desc, read_chunk->num_rows(), final_column);

    return final_column;
}

// Test: INT type round-trip with value assertions
TEST_F(ArrowConverterRoundtripTest, test_int) {
    arrow::Int32Builder builder;
    ASSERT_OK(builder.Append(1));
    ASSERT_OK(builder.Append(42));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK(builder.Append(100));
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_INT), array, 4);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "1");
    ASSERT_EQ(col->debug_item(1), "42");
    ASSERT_TRUE(col->is_null(2));
    ASSERT_EQ(col->debug_item(3), "100");
}

// Test: BIGINT type round-trip
TEST_F(ArrowConverterRoundtripTest, test_bigint) {
    arrow::Int64Builder builder;
    ASSERT_OK(builder.Append(100));
    ASSERT_OK(builder.Append(200));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_BIGINT), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "100");
    ASSERT_EQ(col->debug_item(1), "200");
    ASSERT_TRUE(col->is_null(2));
}

// Test: BOOLEAN type round-trip
TEST_F(ArrowConverterRoundtripTest, test_boolean) {
    arrow::BooleanBuilder builder;
    ASSERT_OK(builder.Append(true));
    ASSERT_OK(builder.Append(false));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_BOOLEAN), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "1");
    ASSERT_EQ(col->debug_item(1), "0");
    ASSERT_TRUE(col->is_null(2));
}

// Test: TINYINT type round-trip
TEST_F(ArrowConverterRoundtripTest, test_tinyint) {
    arrow::Int8Builder builder;
    ASSERT_OK(builder.Append(1));
    ASSERT_OK(builder.Append(-128));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_TINYINT), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "1");
    ASSERT_EQ(col->debug_item(1), "-128");
    ASSERT_TRUE(col->is_null(2));
}

// Test: SMALLINT type round-trip
TEST_F(ArrowConverterRoundtripTest, test_smallint) {
    arrow::Int16Builder builder;
    ASSERT_OK(builder.Append(1));
    ASSERT_OK(builder.Append(-1));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_SMALLINT), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "1");
    ASSERT_EQ(col->debug_item(1), "-1");
    ASSERT_TRUE(col->is_null(2));
}

// Test: FLOAT type round-trip
TEST_F(ArrowConverterRoundtripTest, test_float) {
    arrow::FloatBuilder builder;
    ASSERT_OK(builder.Append(1.5f));
    ASSERT_OK(builder.Append(3.14f));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_FLOAT), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result.value()->size(), 3);
    ASSERT_TRUE(result.value()->is_null(2));
}

// Test: DOUBLE type round-trip
TEST_F(ArrowConverterRoundtripTest, test_double) {
    arrow::DoubleBuilder builder;
    ASSERT_OK(builder.Append(1.5));
    ASSERT_OK(builder.Append(3.14));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_DOUBLE), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result.value()->size(), 3);
    ASSERT_TRUE(result.value()->is_null(2));
}

// Test: VARCHAR type round-trip with value assertions
TEST_F(ArrowConverterRoundtripTest, test_varchar) {
    arrow::StringBuilder builder;
    ASSERT_OK(builder.Append("hello"));
    ASSERT_OK(builder.Append("world"));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor::create_varchar_type(100), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "'hello'");
    ASSERT_EQ(col->debug_item(1), "'world'");
    ASSERT_TRUE(col->is_null(2));
}

// Test: VARBINARY type round-trip
TEST_F(ArrowConverterRoundtripTest, test_varbinary) {
    arrow::BinaryBuilder builder;
    ASSERT_OK(builder.Append("\x01\x02\x03"));
    ASSERT_OK(builder.Append("\xff\xfe"));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_VARBINARY), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result.value()->size(), 3);
    ASSERT_TRUE(result.value()->is_null(2));
}

// Test: DECIMAL128 type round-trip with value assertions - the most risky type per review concern
TEST_F(ArrowConverterRoundtripTest, test_decimal128) {
    TypeDescriptor type_desc;
    type_desc.type = TYPE_DECIMAL128;
    type_desc.precision = 27;
    type_desc.scale = 9;

    // Verify arrow type matches precision/scale
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(type_desc, &arrow_type).ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::DECIMAL128);
    auto* dec_type = down_cast<const arrow::Decimal128Type*>(arrow_type.get());
    ASSERT_EQ(dec_type->precision(), 27);
    ASSERT_EQ(dec_type->scale(), 9);

    // Build decimal array: values are unscaled integers.
    // 123456789000000000 with scale=9 means 123456789.000000000
    // 999999999000000000 with scale=9 means 999999999.000000000
    arrow::Decimal128Builder builder(arrow::decimal128(27, 9));
    ASSERT_OK(builder.Append(arrow::Decimal128("123456789000000000")));
    ASSERT_OK(builder.Append(arrow::Decimal128("999999999000000000")));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(type_desc, array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_FALSE(col->is_null(0));
    ASSERT_FALSE(col->is_null(1));
    ASSERT_TRUE(col->is_null(2));
    ASSERT_EQ(col->size(), 3);
    ASSERT_EQ(col->debug_item(0), "123456789.000000000");
    ASSERT_EQ(col->debug_item(1), "999999999.000000000");
}

// Test: DECIMAL with various precision/scale combinations - full pipeline
TEST_F(ArrowConverterRoundtripTest, test_decimal_various_precision_scale) {
    struct TestCase {
        int precision;
        int scale;
    };
    std::vector<TestCase> cases = {
            {9, 2}, {18, 6}, {27, 9}, {38, 18}, {10, 0},
    };

    for (const auto& tc : cases) {
        TypeDescriptor type_desc;
        type_desc.type = TYPE_DECIMAL128;
        type_desc.precision = tc.precision;
        type_desc.scale = tc.scale;

        // Verify arrow type preserves precision/scale
        std::shared_ptr<arrow::DataType> arrow_type;
        ASSERT_TRUE(convert_to_arrow_type(type_desc, &arrow_type).ok());
        auto* dec_type = down_cast<const arrow::Decimal128Type*>(arrow_type.get());
        ASSERT_EQ(dec_type->precision(), tc.precision);
        ASSERT_EQ(dec_type->scale(), tc.scale);

        // Build a simple decimal array and run full pipeline
        arrow::Decimal128Builder builder(arrow::decimal128(tc.precision, tc.scale));
        ASSERT_OK(builder.Append(arrow::Decimal128(100)));
        ASSERT_OK(builder.AppendNull());
        ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

        auto result = convert_and_verify(type_desc, array, 2);
        ASSERT_TRUE(result.ok()) << "Failed for precision=" << tc.precision << " scale=" << tc.scale << ": "
                                 << result.status();
        ASSERT_FALSE(result.value()->is_null(0));
        ASSERT_TRUE(result.value()->is_null(1));
        ASSERT_NE(result.value()->debug_item(0), "NULL");
    }
}

// Test: DATE type round-trip (with enable_native_arrow_new_type=true, as Paimon uses)
TEST_F(ArrowConverterRoundtripTest, test_date_native) {
    auto original = config::enable_native_arrow_new_type;
    config::enable_native_arrow_new_type = true;

    // Verify arrow type is date32
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(TypeDescriptor(TYPE_DATE), &arrow_type).ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::DATE32);

    arrow::Date32Builder builder;
    ASSERT_OK(builder.Append(18000)); // days since epoch
    ASSERT_OK(builder.Append(19000));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(TypeDescriptor(TYPE_DATE), array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "2019-04-14");
    ASSERT_EQ(col->debug_item(1), "2022-01-08");
    ASSERT_FALSE(col->is_null(0));
    ASSERT_FALSE(col->is_null(1));
    ASSERT_TRUE(col->is_null(2));

    config::enable_native_arrow_new_type = original;
}

// Test: DATETIME/TIMESTAMP type round-trip with RuntimeState for timezone (enable_native_arrow_new_type=true)
TEST_F(ArrowConverterRoundtripTest, test_datetime_timestamp_native) {
    auto original = config::enable_native_arrow_new_type;
    config::enable_native_arrow_new_type = true;

    // Verify arrow type is timestamp(MICRO, timezone)
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(TypeDescriptor(TYPE_DATETIME), &arrow_type, "Asia/Shanghai").ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::TIMESTAMP);
    auto* ts_type = down_cast<const arrow::TimestampType*>(arrow_type.get());
    ASSERT_EQ(ts_type->unit(), arrow::TimeUnit::MICRO);

    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai"),
                                    arrow::default_memory_pool());
    ASSERT_OK(builder.Append(1000000));     // 1 second in micros
    ASSERT_OK(builder.Append(86400000000)); // 1 day in micros
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    // Create RuntimeState with timezone for convert_array_to_column's timestamp rectification
    ObjectPool state_pool;
    TQueryGlobals query_globals;
    query_globals.__set_time_zone("Asia/Shanghai");
    auto* state = state_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), query_globals, nullptr));

    auto result = convert_and_verify(TypeDescriptor(TYPE_DATETIME), array, 3, state);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "1970-01-01 08:00:01");
    ASSERT_EQ(col->debug_item(1), "1970-01-02 08:00:00");
    ASSERT_FALSE(col->is_null(0));
    ASSERT_FALSE(col->is_null(1));
    ASSERT_TRUE(col->is_null(2));
    ASSERT_EQ(col->size(), 3);

    config::enable_native_arrow_new_type = original;
}

// Test: ARRAY<INT> type - full pipeline
TEST_F(ArrowConverterRoundtripTest, test_array_int) {
    TypeDescriptor type_desc(TYPE_ARRAY);
    type_desc.children.emplace_back(TYPE_INT);

    // Verify arrow type is list(int32)
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(type_desc, &arrow_type).ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::LIST);

    // Build list array: [[1, 2], [3], null]
    auto value_builder = std::make_shared<arrow::Int32Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    ASSERT_OK(builder.Append());
    ASSERT_OK(value_builder->Append(1));
    ASSERT_OK(value_builder->Append(2));
    ASSERT_OK(builder.Append());
    ASSERT_OK(value_builder->Append(3));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(type_desc, array, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "[1,2]");
    ASSERT_EQ(col->debug_item(1), "[3]");
    ASSERT_EQ(col->size(), 3);
    ASSERT_TRUE(col->is_null(2));
}

// Test: MAP<VARCHAR, INT> type - full pipeline
TEST_F(ArrowConverterRoundtripTest, test_map_varchar_int) {
    TypeDescriptor type_desc(TYPE_MAP);
    type_desc.children.emplace_back(TypeDescriptor::create_varchar_type(100));
    type_desc.children.emplace_back(TYPE_INT);

    // Verify arrow type is map(utf8, int32)
    std::shared_ptr<arrow::DataType> arrow_type;
    ASSERT_TRUE(convert_to_arrow_type(type_desc, &arrow_type).ok());
    ASSERT_EQ(arrow_type->id(), arrow::Type::MAP);

    // Build map array: [{"a": 1}, null]
    auto key_builder = std::make_shared<arrow::StringBuilder>();
    auto value_builder = std::make_shared<arrow::Int32Builder>();
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
    ASSERT_OK(builder.Append());
    ASSERT_OK(key_builder->Append("a"));
    ASSERT_OK(value_builder->Append(1));
    ASSERT_OK(builder.AppendNull());
    ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());

    auto result = convert_and_verify(type_desc, array, 2);
    ASSERT_TRUE(result.ok()) << result.status();
    auto col = std::move(result).value();
    ASSERT_EQ(col->debug_item(0), "{'a':1}");
    ASSERT_EQ(col->size(), 2);
    ASSERT_TRUE(col->is_null(1));
}

} // namespace starrocks
