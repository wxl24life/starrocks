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

#include <fmt/format.h>
#include <rapidjson/document.h>

#include "common/status.h"
#include "common/statusor.h"

namespace ScalarOperatorUtil {
constexpr auto ARGUMENTS = "a";
constexpr auto BINARY_TYPE = "b";
constexpr auto CHILDREN = "c";
constexpr auto COMPOUND_TYPE = "ct";
constexpr auto FN_NAME = "f";
constexpr auto ITEM_TYPE = "i";
constexpr auto NAME = "n";
constexpr auto OPERATOR_TYPE = "o";
constexpr auto TYPE = "t";
constexpr auto VALUE = "v";

constexpr auto ArrayOperator = "a";
constexpr auto BinaryPredicateOperator = "b";
constexpr auto CallOperator = "ca";
constexpr auto ConstantOperator = "co";
constexpr auto CompoundPredicateOperator = "cp";
constexpr auto ColumnRefOperator = "cr";
} // namespace ScalarOperatorUtil

namespace starrocks {

// Safely extract a string field from a rapidjson object node.
//
// Why: rapidjson::Value::GetString() is UB if the value is not a string — in release
// builds (RAPIDJSON_ASSERT compiled out) it returns an arbitrary pointer that may be
// NULL, and `std::string_view(const char*)` then calls strlen() on it → SIGSEGV.
// This wrapper validates IsObject + HasMember + IsString and uses the (ptr, length)
// string_view constructor, which never calls strlen.
inline StatusOr<std::string_view> safe_get_string_member(const rapidjson::Value& node, const char* key) {
    if (!node.IsObject()) {
        return Status::InternalError(
                fmt::format("global index condition: node is not an object (looking up '{}')", key));
    }
    if (!node.HasMember(key)) {
        return Status::InternalError(fmt::format("global index condition: missing field '{}'", key));
    }
    const auto& v = node[key];
    if (!v.IsString()) {
        return Status::InternalError(fmt::format("global index condition: field '{}' is not a string", key));
    }
    return std::string_view(v.GetString(), v.GetStringLength());
}

// Companion to safe_get_string_member, for fields whose value is expected to be a
// nested JSON value (object or array) handed to a child visitor.
inline StatusOr<const rapidjson::Value*> safe_get_member(const rapidjson::Value& node, const char* key) {
    if (!node.IsObject()) {
        return Status::InternalError(
                fmt::format("global index condition: node is not an object (looking up '{}')", key));
    }
    if (!node.HasMember(key)) {
        return Status::InternalError(fmt::format("global index condition: missing field '{}'", key));
    }
    return &node[key];
}

// Consistent with BinaryType.java
constexpr auto BinaryType_EQ = "eq";
constexpr auto BinaryType_NE = "ne";
constexpr auto BinaryType_LE = "le";
constexpr auto BinaryType_GE = "ge";
constexpr auto BinaryType_LT = "lt";
constexpr auto BinaryType_GT = "gt";

// Consistent with CompoundPredicateOperator.java$CompoundType
constexpr auto CompoundType_AND = "AND";
constexpr auto CompoundType_OR = "OR";
constexpr auto CompoundType_NOT = "NOT";

// Consistent with IndexTable.java
constexpr auto INDEX_RESULT_COLUMN_NAME = "index_result";

template <class R>
class GlobalIndexConditionVisitor {
public:
    virtual ~GlobalIndexConditionVisitor() = default;
    virtual R visitArrayOperator(std::string_view item_type, const rapidjson::Value& children) {
        return visit(ScalarOperatorUtil::ArrayOperator);
    }
    virtual R visitBinaryPredicateOperator(std::string_view binary_type, const rapidjson::Value& children) {
        return visit(ScalarOperatorUtil::BinaryPredicateOperator);
    }
    virtual R visitCallOperator(std::string_view fn_name, const rapidjson::Value& arguments) {
        return visit(ScalarOperatorUtil::CallOperator);
    }
    virtual R visitCompoundPredicateOperator(std::string_view compound_type, const rapidjson::Value& children) {
        return visit(ScalarOperatorUtil::CompoundPredicateOperator);
    }
    virtual R visitConstantOperator(std::string_view type, std::string_view value) {
        return visit(ScalarOperatorUtil::ConstantOperator);
    }
    virtual R visitColumnRefOperator(std::string_view type, std::string_view name) {
        return visit(ScalarOperatorUtil::ColumnRefOperator);
    }
    virtual R visit(std::string_view operator_type) = 0;
};

template <class R>
R visitIndexConditionNode(const rapidjson::Value& node, GlobalIndexConditionVisitor<R>& visitor) {
    auto operator_type_or = safe_get_string_member(node, ScalarOperatorUtil::OPERATOR_TYPE);
    if (!operator_type_or.ok()) {
        return R(operator_type_or.status());
    }
    const std::string_view operator_type = operator_type_or.value();

    if (operator_type == ScalarOperatorUtil::ArrayOperator) {
        auto item_type_or = safe_get_string_member(node, ScalarOperatorUtil::ITEM_TYPE);
        if (!item_type_or.ok()) {
            return R(item_type_or.status());
        }
        auto children_or = safe_get_member(node, ScalarOperatorUtil::CHILDREN);
        if (!children_or.ok()) {
            return R(children_or.status());
        }
        return visitor.visitArrayOperator(item_type_or.value(), *children_or.value());
    }
    if (operator_type == ScalarOperatorUtil::BinaryPredicateOperator) {
        auto binary_type_or = safe_get_string_member(node, ScalarOperatorUtil::BINARY_TYPE);
        if (!binary_type_or.ok()) {
            return R(binary_type_or.status());
        }
        auto children_or = safe_get_member(node, ScalarOperatorUtil::CHILDREN);
        if (!children_or.ok()) {
            return R(children_or.status());
        }
        return visitor.visitBinaryPredicateOperator(binary_type_or.value(), *children_or.value());
    }
    if (operator_type == ScalarOperatorUtil::CallOperator) {
        auto fn_name_or = safe_get_string_member(node, ScalarOperatorUtil::FN_NAME);
        if (!fn_name_or.ok()) {
            return R(fn_name_or.status());
        }
        auto arguments_or = safe_get_member(node, ScalarOperatorUtil::ARGUMENTS);
        if (!arguments_or.ok()) {
            return R(arguments_or.status());
        }
        return visitor.visitCallOperator(fn_name_or.value(), *arguments_or.value());
    }
    if (operator_type == ScalarOperatorUtil::CompoundPredicateOperator) {
        auto compound_type_or = safe_get_string_member(node, ScalarOperatorUtil::COMPOUND_TYPE);
        if (!compound_type_or.ok()) {
            return R(compound_type_or.status());
        }
        auto children_or = safe_get_member(node, ScalarOperatorUtil::CHILDREN);
        if (!children_or.ok()) {
            return R(children_or.status());
        }
        return visitor.visitCompoundPredicateOperator(compound_type_or.value(), *children_or.value());
    }
    if (operator_type == ScalarOperatorUtil::ConstantOperator) {
        auto type_or = safe_get_string_member(node, ScalarOperatorUtil::TYPE);
        if (!type_or.ok()) {
            return R(type_or.status());
        }
        auto value_or = safe_get_string_member(node, ScalarOperatorUtil::VALUE);
        if (!value_or.ok()) {
            return R(value_or.status());
        }
        return visitor.visitConstantOperator(type_or.value(), value_or.value());
    }
    if (operator_type == ScalarOperatorUtil::ColumnRefOperator) {
        auto type_or = safe_get_string_member(node, ScalarOperatorUtil::TYPE);
        if (!type_or.ok()) {
            return R(type_or.status());
        }
        auto name_or = safe_get_string_member(node, ScalarOperatorUtil::NAME);
        if (!name_or.ok()) {
            return R(name_or.status());
        }
        return visitor.visitColumnRefOperator(type_or.value(), name_or.value());
    }
    return visitor.visit(operator_type);
}

} // namespace starrocks
