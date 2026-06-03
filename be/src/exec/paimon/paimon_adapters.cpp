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

#include "paimon_adapters.h"

#include "column/column.h"
#include "column/datum.h"
#include "exprs/literal.h"
#include "fs/paimon/paimon_file_system.h"
#include "runtime/types.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"

namespace starrocks {

std::unordered_map<LogicalType, paimon::FieldType> logical_type_to_paimon_field_type = {
        {TYPE_BOOLEAN, paimon::FieldType::BOOLEAN},   {TYPE_TINYINT, paimon::FieldType::TINYINT},
        {TYPE_SMALLINT, paimon::FieldType::SMALLINT}, {TYPE_INT, paimon::FieldType::INT},
        {TYPE_BIGINT, paimon::FieldType::BIGINT},     {TYPE_FLOAT, paimon::FieldType::FLOAT},
        {TYPE_DOUBLE, paimon::FieldType::DOUBLE},     {TYPE_CHAR, paimon::FieldType::STRING},
        {TYPE_VARCHAR, paimon::FieldType::STRING},
};

std::unordered_map<LogicalType, std::shared_ptr<arrow::DataType>> logical_type_to_paimon_arrow_data_type = {
        {TYPE_BOOLEAN, arrow::boolean()}, {TYPE_TINYINT, arrow::int8()}, {TYPE_SMALLINT, arrow::int16()},
        {TYPE_INT, arrow::int32()},       {TYPE_BIGINT, arrow::int64()}, {TYPE_FLOAT, arrow::float32()},
        {TYPE_DOUBLE, arrow::float64()},  {TYPE_CHAR, arrow::utf8()},    {TYPE_VARCHAR, arrow::utf8()}};

using PaimonLiteralFromDatumGetter = std::function<StatusOr<paimon::Literal>(const Datum&)>;
std::unordered_map<paimon::FieldType, PaimonLiteralFromDatumGetter>
        paimon_field_type_to_paimon_literal_from_expr_getter = {
                {paimon::FieldType::BOOLEAN,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> {
                     return paimon::Literal(datum.get_int8() != 0);
                 }},
                {paimon::FieldType::TINYINT,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_int8()); }},
                {paimon::FieldType::SMALLINT,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_int16()); }},
                {paimon::FieldType::INT,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_int32()); }},
                {paimon::FieldType::BIGINT,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_int64()); }},
                {paimon::FieldType::FLOAT,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_float()); }},
                {paimon::FieldType::DOUBLE,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> { return paimon::Literal(datum.get_double()); }},
                {paimon::FieldType::STRING,
                 [](const Datum& datum) -> StatusOr<paimon::Literal> {
                     const Slice& slice = datum.get_slice();
                     return paimon::Literal(paimon::FieldType::STRING, slice.data, slice.size);
                 }},
};

using PaimonLiteralFromJsonGetter = std::function<paimon::Literal(const rapidjson::Value&)>;
StringCaseMap<PaimonLiteralFromJsonGetter> type_name_to_paimon_literal_from_json_getter = {
        {"BOOLEAN", [](const rapidjson::Value& value) -> paimon::Literal { return paimon::Literal(value.GetBool()); }},
        {"TINYINT",
         [](const rapidjson::Value& value) -> paimon::Literal {
             return paimon::Literal(static_cast<int8_t>(value.Get<int>()));
         }},
        {"SMALLINT",
         [](const rapidjson::Value& value) -> paimon::Literal {
             return paimon::Literal(static_cast<int16_t>(value.Get<int>()));
         }},
        {"INT", [](const rapidjson::Value& value) -> paimon::Literal { return paimon::Literal(value.Get<int32_t>()); }},
        {"BIGINT",
         [](const rapidjson::Value& value) -> paimon::Literal { return paimon::Literal(value.Get<int64_t>()); }},
        {"FLOAT", [](const rapidjson::Value& value) -> paimon::Literal { return paimon::Literal(value.Get<float>()); }},
        {"DOUBLE",
         [](const rapidjson::Value& value) -> paimon::Literal { return paimon::Literal(value.Get<double>()); }},
        {"VARCHAR", [](const rapidjson::Value& value) -> paimon::Literal {
             // Use the (ptr, length) constructor — std::string_view(const char*) would call
             // strlen() on the result of GetString(), which is a NULL pointer for any non-string
             // rapidjson::Value in a release build (RAPIDJSON_ASSERT compiled out) and crashes.
             if (!value.IsString()) {
                 return paimon::Literal(paimon::FieldType::STRING, "", 0);
             }
             return paimon::Literal(paimon::FieldType::STRING, value.GetString(), value.GetStringLength());
         }}};

StringCaseMap<paimon::VectorSearch::DistanceType> fn_name_to_paimon_vector_search_distance_type = {
        {"approx_cosine_similarity", paimon::VectorSearch::DistanceType::COSINE},
        {"approx_inner_product", paimon::VectorSearch::DistanceType::INNER_PRODUCT},
        {"approx_l2_distance", paimon::VectorSearch::DistanceType::EUCLIDEAN},
};

StatusOr<paimon::FieldType> translateToPaimonType(const TypeDescriptor& type) {
    if (logical_type_to_paimon_field_type.contains(type.type)) {
        return logical_type_to_paimon_field_type[type.type];
    }
    return Status::NotSupported("translateToPaimonType fail, type: " + type.debug_string());
}

StatusOr<std::shared_ptr<arrow::DataType>> translateToPaimonArrowType(const TypeDescriptor& type) {
    if (logical_type_to_paimon_arrow_data_type.contains(type.type)) {
        return logical_type_to_paimon_arrow_data_type[type.type];
    }
    return Status::NotSupported(fmt::format("translateToPaimonArrowType fail, type: {}", type.debug_string()));
}

StatusOr<paimon::Literal> translateToPaimonLiteral(Expr* literal) {
    if (!literal) {
        return Status::InvalidArgument("translateToPaimonLiteral fail, literal is null");
    }
    const TExprNodeType::type node_type = literal->node_type();
    ASSIGN_OR_RETURN(const paimon::FieldType data_type, translateToPaimonType(literal->type()));
    // null literal
    if (node_type == TExprNodeType::type::NULL_LITERAL) {
        return paimon::Literal(data_type);
    }
    // function call
    if (node_type == TExprNodeType::type::FUNCTION_CALL) {
        return Status::NotSupported("translateToPaimonLiteral fail, not support function call: " +
                                    literal->debug_string());
    }
    if (auto* vectorized_literal = dynamic_cast<VectorizedLiteral*>(literal)) {
        ASSIGN_OR_RETURN(const auto ptr, vectorized_literal->evaluate_checked(nullptr, nullptr));
        RETURN_IF(ptr->only_null(), paimon::Literal(data_type));
        const Datum& datum = ptr->get(0);
        if (paimon_field_type_to_paimon_literal_from_expr_getter.contains(data_type)) {
            return paimon_field_type_to_paimon_literal_from_expr_getter[data_type](datum);
        }
        return Status::NotSupported(fmt::format("translateToPaimonLiteral fail, data_type:{}, literal:{} ",
                                                static_cast<int>(data_type), literal->debug_string()));
    }
    return Status::NotSupported("translateToPaimonLiteral fail, cannot cast literal to vectorized_literal: " +
                                literal->debug_string());
}

StatusOr<paimon::Literal> translateToPaimonLiteral(const std::string& type_name, const rapidjson::Value& value) {
    if (type_name_to_paimon_literal_from_json_getter.contains(type_name)) {
        return type_name_to_paimon_literal_from_json_getter[type_name](value);
    }
    return Status::NotSupported(fmt::format("translateToPaimonLiteral fail, not support type_name: {}", type_name));
}

StatusOr<paimon::VectorSearch::DistanceType> translateToPaimonVectorSearchDistanceType(const std::string& fn_name) {
    if (fn_name_to_paimon_vector_search_distance_type.contains(fn_name)) {
        return fn_name_to_paimon_vector_search_distance_type[fn_name];
    }
    return Status::NotSupported(
            fmt::format("translateToPaimonVectorSearchDistanceType fail, not support fn_name: {}", fn_name));
}

} // namespace starrocks
