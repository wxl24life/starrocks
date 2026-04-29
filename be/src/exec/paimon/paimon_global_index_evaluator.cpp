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

#include "paimon_global_index_evaluator.h"

#include <fmt/format.h>
#include <paimon/utils/roaring_bitmap64.h>

#include "paimon_adapters.h"

namespace starrocks {

bool equalsIgnoreCase(std::string_view lhs, std::string_view rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    return std::ranges::equal(lhs, rhs, [](const char a, const char b) { return std::tolower(a) == std::tolower(b); });
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> mergeReadColumnIndexes(
        const std::vector<std::shared_ptr<paimon::GlobalIndexReader>>& readers,
        const std::function<StatusOr<std::shared_ptr<paimon::GlobalIndexResult>>(
                std::shared_ptr<paimon::GlobalIndexReader>)>& func) {
    std::shared_ptr<paimon::GlobalIndexResult> res = nullptr;
    for (const auto& reader : readers) {
        ASSIGN_OR_RETURN(auto partial, func(reader));
        if (res == nullptr) {
            res = partial;
        } else {
            ASSIGN_OR_RETURN_PAIMON(res, res->And(partial));
        }
    }
    return res;
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexPredicateEvaluator::visitBinaryPredicateOperator(
        std::string_view binary_type, const rapidjson::Value& children) {
    const auto& left = children[0];
    const auto& right = children[1];
    if (left[ScalarOperatorUtil::OPERATOR_TYPE] == ScalarOperatorUtil::ColumnRefOperator &&
        right[ScalarOperatorUtil::OPERATOR_TYPE] == ScalarOperatorUtil::ConstantOperator) {
        const auto column_name = std::string_view(left[ScalarOperatorUtil::NAME].GetString());
        const auto constant_type = std::string_view(right[ScalarOperatorUtil::TYPE].GetString());
        ASSIGN_OR_RETURN(const auto readers, _readers_getter(column_name));
        ASSIGN_OR_RETURN(const auto paimon_literal,
                         translateToPaimonLiteral(std::string(constant_type), right[ScalarOperatorUtil::VALUE]));
        if (equalsIgnoreCase(binary_type, BinaryType_EQ)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitEqual(paimon_literal));
                        return result;
                    });
        }
        if (equalsIgnoreCase(binary_type, BinaryType_NE)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitNotEqual(paimon_literal));
                        return result;
                    });
        }
        if (equalsIgnoreCase(binary_type, BinaryType_LT)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitLessThan(paimon_literal));
                        return result;
                    });
        }
        if (equalsIgnoreCase(binary_type, BinaryType_LE)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitLessOrEqual(paimon_literal));
                        return result;
                    });
        }
        if (equalsIgnoreCase(binary_type, BinaryType_GT)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitGreaterThan(paimon_literal));
                        return result;
                    });
        }
        if (equalsIgnoreCase(binary_type, BinaryType_GE)) {
            return mergeReadColumnIndexes(
                    readers, [&](const auto& reader) -> StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> {
                        ASSIGN_OR_RETURN_PAIMON(auto result, reader->VisitGreaterOrEqual(paimon_literal));
                        return result;
                    });
        }
        return Status::NotSupported(fmt::format("not support binary type: {}", binary_type));
    }
    // FE side IndexAnalyzer::checkBinaryPredicateAndGetColumnName only emits (column, literal)
    // shape into the prefilter, so reaching here means a contract violation rather than a
    // user-facing parse error. Returning nullptr would NPE inside visitCompoundPredicateOperator.
    return Status::InternalError(fmt::format(
            "binary predicate must be (column, literal); FE/BE prefilter contract violated: {}", binary_type));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>>
PaimonGlobalIndexPredicateEvaluator::visitCompoundPredicateOperator(std::string_view compound_type,
                                                                    const rapidjson::Value& children) {
    if (compound_type == CompoundType_AND) {
        ASSIGN_OR_RETURN(const auto left, visitIndexConditionNode(children[0], *this));
        ASSIGN_OR_RETURN(const auto right, visitIndexConditionNode(children[1], *this));
        ASSIGN_OR_RETURN_PAIMON(auto res, left->And(right));
        return res;
    }
    if (compound_type == CompoundType_OR) {
        ASSIGN_OR_RETURN(const auto left, visitIndexConditionNode(children[0], *this));
        ASSIGN_OR_RETURN(const auto right, visitIndexConditionNode(children[1], *this));
        ASSIGN_OR_RETURN_PAIMON(auto res, left->Or(right));
        return res;
    }
    if (compound_type == CompoundType_NOT) {
        return Status::InternalError("`not` should be pushed down in the FE.");
    }
    return Status::NotSupported(fmt::format("unknown compound type: {}", compound_type));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexPredicateEvaluator::visitConstantOperator(
        std::string_view type, std::string_view value) {
    return Status::NotSupported("not support constant bool");
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexPredicateEvaluator::visitColumnRefOperator(
        std::string_view type, std::string_view name) {
    return Status::NotSupported("not support bool column");
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexPredicateEvaluator::visit(
        std::string_view op_type) {
    return Status::NotSupported(fmt::format("not support operatorType: {}", op_type));
}

// -------------------------------- topN --------------------------------

// Consistent with IndexAnalyzer.java
bool isAnnFunction(const std::string_view fn_name) {
    return equalsIgnoreCase(fn_name, "approx_cosine_similarity") || equalsIgnoreCase(fn_name, "approx_inner_product") ||
           equalsIgnoreCase(fn_name, "approx_l2_distance");
}

std::vector<float> getQueryVector(const rapidjson::Value& value) {
    std::vector<float> query;
    const auto array = value[ScalarOperatorUtil::CHILDREN].GetArray();
    query.reserve(array.Size());
    for (const auto& element : array) {
        const auto& v = element[ScalarOperatorUtil::VALUE];
        query.push_back(v.Get<float>());
    }
    return query;
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexTopNEvaluator::visitCallOperator(
        std::string_view fn_name, const rapidjson::Value& arguments) {
    if (isAnnFunction(fn_name)) {
        const auto column_name = std::string_view(arguments[0][ScalarOperatorUtil::NAME].GetString());
        const auto query = getQueryVector(arguments[1]);
        ASSIGN_OR_RETURN(const auto readers, _readers_getter(column_name));

        // PreFilter: capture the BitmapGlobalIndexResult shared_ptr by value so the underlying
        // RoaringBitmap64 stays alive even if paimon SDK retains the lambda past this scope.
        paimon::VectorSearch::PreFilter prefilter = nullptr;
        if (_predicate_index_result) {
            auto bitmap_holder = _predicate_index_result;
            prefilter = [bitmap_holder](const int64_t id) -> bool {
                auto bitmap = bitmap_holder->GetBitmap();
                return bitmap.ok() && bitmap.value()->Contains(id);
            };
        }

        // Tag predicate is not pushed down yet.
        std::shared_ptr<paimon::Predicate> predicate = nullptr;

        ASSIGN_OR_RETURN(auto distance_type, translateToPaimonVectorSearchDistanceType(std::string(fn_name)));

        // lumina index options with default. Default search.list_size matches FE-side
        // PaimonGlobalIndexBackendSelector (1024); upscale linearly when localN exceeds it.
        std::map<std::string, std::string> index_options = {{"lumina.diskann.search.list_size", "1024"},
                                                            {"lumina.search.parallel_number", "5"},
                                                            {"lumina.diskann.search.beam_width", "4"}};
        if (_n > 1024) {
            index_options["lumina.diskann.search.list_size"] = std::to_string(_n * 3 / 2);
        }

        const auto vector_search = std::make_shared<paimon::VectorSearch>(std::string(column_name), _n, query,
                                                                          prefilter, predicate, distance_type,
                                                                          index_options);

        if (readers.size() != 1) {
            return Status::InternalError(fmt::format("index reader number of {} is {}", column_name, readers.size()));
        }

        std::shared_ptr<paimon::ScoredGlobalIndexResult> result = nullptr;
        ASSIGN_OR_RETURN_PAIMON(result, readers[0]->VisitVectorSearch(vector_search));
        return result;
    }
    return Status::NotSupported(
            fmt::format("PaimonGlobalIndexTopNEvaluator fail, "
                        "not support function: {}",
                        fn_name));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexTopNEvaluator::visitConstantOperator(
        std::string_view type, std::string_view value) {
    return Status::NotSupported("not support constant");
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexTopNEvaluator::visitColumnRefOperator(
        std::string_view type, std::string_view name) {
    return Status::NotSupported("not support column");
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexTopNEvaluator::visit(std::string_view op_type) {
    return Status::NotSupported(fmt::format("not support operatorType: {}", op_type));
}

} // namespace starrocks
