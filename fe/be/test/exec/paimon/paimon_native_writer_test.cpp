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

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/object_pool.h"
#include "connector/utils.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

namespace {

// Mock Expr that returns a pre-configured column
class MockColumnExpr : public Expr {
public:
    MockColumnExpr(TypeDescriptor type, ColumnPtr column, bool nullable)
            : Expr(std::move(type), nullable), _column(std::move(column)) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _column->clone(); }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MockColumnExpr(*this)); }

    bool is_constant() const override { return false; }

private:
    ColumnPtr _column;
};

// Helper to create nullable column with NULL value
ColumnPtr create_null_column() {
    auto data_column = Int32Column::create();
    data_column->append(0);
    auto null_column = NullColumn::create();
    null_column->append(1); // NULL
    auto col = NullableColumn::create(std::move(data_column), std::move(null_column));
    col->update_has_null();
    return col;
}

// Helper to create nullable column with non-NULL int value
ColumnPtr create_int_column_nullable(int32_t value) {
    auto data_column = Int32Column::create();
    data_column->append(value);
    auto null_column = NullColumn::create();
    null_column->append(0); // NOT NULL
    return NullableColumn::create(std::move(data_column), std::move(null_column));
}

// Helper to create non-nullable int column
ColumnPtr create_int_column(int32_t value) {
    auto data_column = Int32Column::create();
    data_column->append(value);
    return data_column;
}

// Helper to create nullable string column with NULL
ColumnPtr create_null_string_column() {
    auto data_column = BinaryColumn::create();
    data_column->append("");
    auto null_column = NullColumn::create();
    null_column->append(1); // NULL
    auto col = NullableColumn::create(std::move(data_column), std::move(null_column));
    col->update_has_null();
    return col;
}

// Helper to create nullable string column with value
ColumnPtr create_string_column_nullable(const std::string& value) {
    auto data_column = BinaryColumn::create();
    data_column->append(value);
    auto null_column = NullColumn::create();
    null_column->append(0); // NOT NULL
    return NullableColumn::create(std::move(data_column), std::move(null_column));
}

// Simulates the extract_partition_values logic from PaimonNativeWriter
StatusOr<std::map<std::string, std::string>> extract_partition_values_for_test(
        const std::vector<std::string>& partition_keys, const std::vector<ExprContext*>& partition_exprs,
        const std::string& partition_default_name, const ChunkPtr& chunk) {
    std::map<std::string, std::string> partition_values;
    for (int i = 0; i < partition_keys.size(); ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, partition_exprs[i]->evaluate(chunk.get()));
        auto type = partition_exprs[i]->root()->type();
        if (column->has_null() && column->get(0).is_null()) {
            // Use partition.default-name (e.g. "__DEFAULT_PARTITION__") for NULL partition values
            partition_values.emplace(partition_keys[i], partition_default_name);
        } else {
            ASSIGN_OR_RETURN(auto value, connector::HiveUtils::column_value(type, column, 0));
            partition_values.emplace(partition_keys[i], value);
        }
    }
    return partition_values;
}

} // namespace

class PaimonNativeWriterExtractPartitionTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = std::make_shared<RuntimeState>();
        _pool = std::make_unique<ObjectPool>();
    }

    void TearDown() override {
        for (auto* ctx : _expr_contexts) {
            ctx->close(_runtime_state.get());
        }
        _expr_contexts.clear();
        _pool.reset();
        _runtime_state.reset();
    }

protected:
    ExprContext* create_expr_context(TypeDescriptor type, ColumnPtr column, bool nullable) {
        auto* expr = _pool->add(new MockColumnExpr(std::move(type), std::move(column), nullable));
        auto* ctx = _pool->add(new ExprContext(expr));
        EXPECT_OK(ctx->prepare(_runtime_state.get()));
        EXPECT_OK(ctx->open(_runtime_state.get()));
        _expr_contexts.push_back(ctx);
        return ctx;
    }

    ChunkPtr create_empty_chunk() {
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "col", get_type_info(TYPE_INT), true));
        auto schema = std::make_shared<Schema>(fields);
        return std::make_shared<Chunk>(Columns{}, schema);
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<ObjectPool> _pool;
    std::vector<ExprContext*> _expr_contexts;
};

// Test: NULL partition value should use default partition name
TEST_F(PaimonNativeWriterExtractPartitionTest, TestNullPartitionValue) {
    std::vector<std::string> partition_keys = {"date_col"};
    std::vector<ExprContext*> partition_exprs;

    // Create expr that returns NULL
    auto null_column = create_null_column();
    auto* ctx = create_expr_context(TypeDescriptor(TYPE_INT), null_column, true);
    partition_exprs.push_back(ctx);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__DEFAULT_PARTITION__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(1, partition_values.size());
    ASSERT_EQ("__DEFAULT_PARTITION__", partition_values["date_col"]);
}

// Test: Non-NULL partition value should use actual value
TEST_F(PaimonNativeWriterExtractPartitionTest, TestNonNullPartitionValue) {
    std::vector<std::string> partition_keys = {"date_col"};
    std::vector<ExprContext*> partition_exprs;

    // Create expr that returns non-NULL value
    auto value_column = create_int_column_nullable(20240101);
    auto* ctx = create_expr_context(TypeDescriptor(TYPE_INT), value_column, true);
    partition_exprs.push_back(ctx);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__DEFAULT_PARTITION__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(1, partition_values.size());
    ASSERT_EQ("20240101", partition_values["date_col"]);
}

// Test: Custom partition default name
TEST_F(PaimonNativeWriterExtractPartitionTest, TestCustomPartitionDefaultName) {
    std::vector<std::string> partition_keys = {"region"};
    std::vector<ExprContext*> partition_exprs;

    // Create expr that returns NULL
    auto null_column = create_null_string_column();
    auto* ctx = create_expr_context(TypeDescriptor(TYPE_VARCHAR), null_column, true);
    partition_exprs.push_back(ctx);

    auto chunk = create_empty_chunk();

    // Use custom partition default name
    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "NULL_REGION", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(1, partition_values.size());
    ASSERT_EQ("NULL_REGION", partition_values["region"]);
}

// Test: Multiple partition keys with mix of NULL and non-NULL values
TEST_F(PaimonNativeWriterExtractPartitionTest, TestMultiplePartitionKeys) {
    std::vector<std::string> partition_keys = {"date_col", "region"};
    std::vector<ExprContext*> partition_exprs;

    // date_col is NULL
    auto null_column = create_null_column();
    auto* ctx1 = create_expr_context(TypeDescriptor(TYPE_INT), null_column, true);
    partition_exprs.push_back(ctx1);

    // region is non-NULL
    auto region_column = create_string_column_nullable("us-west");
    auto* ctx2 = create_expr_context(TypeDescriptor(TYPE_VARCHAR), region_column, true);
    partition_exprs.push_back(ctx2);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__NULL__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(2, partition_values.size());
    ASSERT_EQ("__NULL__", partition_values["date_col"]);
    ASSERT_EQ("us-west", partition_values["region"]);
}

// Test: All partition values are non-NULL
TEST_F(PaimonNativeWriterExtractPartitionTest, TestAllNonNullValues) {
    std::vector<std::string> partition_keys = {"year", "month", "day"};
    std::vector<ExprContext*> partition_exprs;

    auto year_col = create_int_column_nullable(2024);
    auto* ctx1 = create_expr_context(TypeDescriptor(TYPE_INT), year_col, true);
    partition_exprs.push_back(ctx1);

    auto month_col = create_int_column_nullable(3);
    auto* ctx2 = create_expr_context(TypeDescriptor(TYPE_INT), month_col, true);
    partition_exprs.push_back(ctx2);

    auto day_col = create_int_column_nullable(18);
    auto* ctx3 = create_expr_context(TypeDescriptor(TYPE_INT), day_col, true);
    partition_exprs.push_back(ctx3);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__DEFAULT_PARTITION__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(3, partition_values.size());
    ASSERT_EQ("2024", partition_values["year"]);
    ASSERT_EQ("3", partition_values["month"]);
    ASSERT_EQ("18", partition_values["day"]);
}

// Test: All partition values are NULL
TEST_F(PaimonNativeWriterExtractPartitionTest, TestAllNullValues) {
    std::vector<std::string> partition_keys = {"col1", "col2"};
    std::vector<ExprContext*> partition_exprs;

    auto null_col1 = create_null_column();
    auto* ctx1 = create_expr_context(TypeDescriptor(TYPE_INT), null_col1, true);
    partition_exprs.push_back(ctx1);

    auto null_col2 = create_null_string_column();
    auto* ctx2 = create_expr_context(TypeDescriptor(TYPE_VARCHAR), null_col2, true);
    partition_exprs.push_back(ctx2);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "EMPTY", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(2, partition_values.size());
    ASSERT_EQ("EMPTY", partition_values["col1"]);
    ASSERT_EQ("EMPTY", partition_values["col2"]);
}

// Test: Empty partition keys
TEST_F(PaimonNativeWriterExtractPartitionTest, TestEmptyPartitionKeys) {
    std::vector<std::string> partition_keys;
    std::vector<ExprContext*> partition_exprs;
    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__DEFAULT_PARTITION__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(0, partition_values.size());
}

// Test: Non-nullable column (no null flag)
TEST_F(PaimonNativeWriterExtractPartitionTest, TestNonNullableColumn) {
    std::vector<std::string> partition_keys = {"id"};
    std::vector<ExprContext*> partition_exprs;

    // Non-nullable int column
    auto id_col = create_int_column(12345);
    auto* ctx = create_expr_context(TypeDescriptor(TYPE_INT), id_col, false);
    partition_exprs.push_back(ctx);

    auto chunk = create_empty_chunk();

    auto result = extract_partition_values_for_test(partition_keys, partition_exprs, "__DEFAULT_PARTITION__", chunk);
    ASSERT_OK(result.status());
    auto partition_values = result.value();
    ASSERT_EQ(1, partition_values.size());
    ASSERT_EQ("12345", partition_values["id"]);
}

} // namespace starrocks
