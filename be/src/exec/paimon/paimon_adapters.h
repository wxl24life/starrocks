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

#include <arrow/type.h>
#include <fmt/format.h>
#include <gutil/macros.h>
#include <paimon/predicate/vector_search.h>

#include "exprs/expr.h"
#include "fs/fs.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "storage/rowset/default_value_column_iterator.h"

namespace starrocks {

#define RETURN_IF_ERROR_PAIMON(stmt)                                                \
    do {                                                                            \
        auto&& status__ = (stmt);                                                   \
        if (UNLIKELY(!status__.ok())) {                                             \
            return Status::InternalError(status__.status().message())               \
                    .clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt)); \
        }                                                                           \
    } while (false)

#define ASSIGN_OR_RETURN_PAIMON_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                             \
    RETURN_IF_ERROR_PAIMON(varname);                    \
    lhs = std::move(varname).value()

#define ASSIGN_OR_RETURN_PAIMON(lhs, rhs) ASSIGN_OR_RETURN_PAIMON_IMPL(VARNAME_LINENUM(value_or_err), lhs, rhs)

#define RETURN_IF_ERROR_WITH_MSG_PAIMON(stmt, msg_stmt)                                                       \
    do {                                                                                                      \
        auto&& status__ = (stmt);                                                                             \
        if (UNLIKELY(!status__.ok())) {                                                                       \
            auto&& msg = (msg_stmt);                                                                          \
            return Status::InternalError(fmt::format("{}, caused by:\n{}", msg, status__.status().message())) \
                    .clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt));                           \
        }                                                                                                     \
    } while (false)

#define ASSIGN_OR_RETURN_WITH_MSG_PAIMON_IMPL(varname, lhs, rhs, msg_stmt) \
    auto&& varname = (rhs);                                                \
    RETURN_IF_ERROR_WITH_MSG_PAIMON(varname, msg_stmt);                    \
    lhs = std::move(varname).value()

// ASSIGN_OR_RETURN_PAIMON_WITH_MSG: Similar to ASSIGN_OR_RETURN_PAIMON, but allows custom message
// When an error occurs, the message will be: "msg, caused by: status__.status().message()"
#define ASSIGN_OR_RETURN_WITH_MSG_PAIMON(lhs, rhs, msg_stmt) \
    ASSIGN_OR_RETURN_WITH_MSG_PAIMON_IMPL(VARNAME_LINENUM(value_or_err), lhs, rhs, msg_stmt)

StatusOr<paimon::FieldType> translateToPaimonType(const TypeDescriptor& type);

StatusOr<std::shared_ptr<arrow::DataType>> translateToPaimonArrowType(const TypeDescriptor& type);

StatusOr<paimon::Literal> translateToPaimonLiteral(Expr* literal);

StatusOr<paimon::Literal> translateToPaimonLiteral(const std::string& type_name, const rapidjson::Value& value);

StatusOr<paimon::VectorSearch::DistanceType> translateToPaimonVectorSearchDistanceType(const std::string& fn_name);

} // namespace starrocks
