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

#include <rapidjson/document.h>

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
    const std::string_view operator_type(node[ScalarOperatorUtil::OPERATOR_TYPE].GetString());
    if (operator_type == ScalarOperatorUtil::ArrayOperator) {
        return visitor.visitArrayOperator(std::string_view(node[ScalarOperatorUtil::ITEM_TYPE].GetString()),
                                          node[ScalarOperatorUtil::CHILDREN]);
    }
    if (operator_type == ScalarOperatorUtil::BinaryPredicateOperator) {
        return visitor.visitBinaryPredicateOperator(std::string_view(node[ScalarOperatorUtil::BINARY_TYPE].GetString()),
                                                    node[ScalarOperatorUtil::CHILDREN]);
    }
    if (operator_type == ScalarOperatorUtil::CallOperator) {
        return visitor.visitCallOperator(std::string_view(node[ScalarOperatorUtil::FN_NAME].GetString()),
                                         node[ScalarOperatorUtil::ARGUMENTS]);
    }
    if (operator_type == ScalarOperatorUtil::CompoundPredicateOperator) {
        return visitor.visitCompoundPredicateOperator(
                std::string_view(node[ScalarOperatorUtil::COMPOUND_TYPE].GetString()),
                node[ScalarOperatorUtil::CHILDREN]);
    }
    if (operator_type == ScalarOperatorUtil::ConstantOperator) {
        return visitor.visitConstantOperator(std::string_view(node[ScalarOperatorUtil::TYPE].GetString()),
                                             std::string_view(node[ScalarOperatorUtil::VALUE].GetString()));
    }
    if (operator_type == ScalarOperatorUtil::ColumnRefOperator) {
        return visitor.visitColumnRefOperator(std::string_view(node[ScalarOperatorUtil::TYPE].GetString()),
                                              std::string_view(node[ScalarOperatorUtil::NAME].GetString()));
    }
    return visitor.visit(operator_type);
}

} // namespace starrocks
