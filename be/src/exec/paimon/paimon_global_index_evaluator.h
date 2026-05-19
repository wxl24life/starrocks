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

#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/global_index_reader.h>
#include <paimon/global_index/global_index_result.h>

#include "common/statusor.h"
#include "global_index_common.h"
#include "paimon_adapters.h"

namespace starrocks {

class PaimonGlobalIndexConditionVisitor
        : public GlobalIndexConditionVisitor<StatusOr<std::shared_ptr<paimon::GlobalIndexResult>>> {
public:
    // (column_name, params) -> readers
    using ReadersGetter =
            std::function<StatusOr<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>>(std::string_view)>;
    explicit PaimonGlobalIndexConditionVisitor(const ReadersGetter& readers_getter) : _readers_getter(readers_getter) {}
    ~PaimonGlobalIndexConditionVisitor() override = default;

protected:
    ReadersGetter _readers_getter;
};

class PaimonGlobalIndexPredicateEvaluator final : public PaimonGlobalIndexConditionVisitor {
public:
    PaimonGlobalIndexPredicateEvaluator(const ReadersGetter& readers_getter)
            : PaimonGlobalIndexConditionVisitor(readers_getter) {}
    ~PaimonGlobalIndexPredicateEvaluator() override = default;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitBinaryPredicateOperator(
            std::string_view binary_type, const rapidjson::Value& children) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitCallOperator(std::string_view fn_name,
                                                                           const rapidjson::Value& arguments) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitConstantOperator(std::string_view type,
                                                                               std::string_view value) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitCompoundPredicateOperator(
            std::string_view compound_type, const rapidjson::Value& children) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitColumnRefOperator(std::string_view type,
                                                                                std::string_view name) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visit(std::string_view type) override;
};

class PaimonGlobalIndexTopNEvaluator final : public PaimonGlobalIndexConditionVisitor {
public:
    // Hold a shared_ptr (not a raw RoaringBitmap64*) so the bitmap stays alive for any
    // PreFilter lambda the paimon SDK may retain past VisitVectorSearch.
    PaimonGlobalIndexTopNEvaluator(const ReadersGetter& readers_getter,
                                   std::shared_ptr<paimon::BitmapGlobalIndexResult> predicate_index_result,
                                   const int32_t n)
            : PaimonGlobalIndexConditionVisitor(readers_getter),
              _predicate_index_result(std::move(predicate_index_result)),
              _n(n) {}

    ~PaimonGlobalIndexTopNEvaluator() override = default;

    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitCallOperator(std::string_view fn_name,
                                                                           const rapidjson::Value& arguments) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitConstantOperator(std::string_view type,
                                                                               std::string_view value) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visitColumnRefOperator(std::string_view type,
                                                                                std::string_view name) override;
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> visit(std::string_view op_type) override;

private:
    std::shared_ptr<paimon::BitmapGlobalIndexResult> _predicate_index_result;
    int32_t _n;
};

} // namespace starrocks
