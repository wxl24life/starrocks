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

#include "common/config.h"
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
    if (!children.IsArray() || children.Size() < 2) {
        return Status::InternalError(
                fmt::format("binary predicate children must be an array of >=2 elements, got size={}",
                            children.IsArray() ? children.Size() : 0));
    }
    const auto& left = children[0];
    const auto& right = children[1];
    auto left_op_or = safe_get_string_member(left, ScalarOperatorUtil::OPERATOR_TYPE);
    auto right_op_or = safe_get_string_member(right, ScalarOperatorUtil::OPERATOR_TYPE);
    if (left_op_or.ok() && right_op_or.ok() && left_op_or.value() == ScalarOperatorUtil::ColumnRefOperator &&
        right_op_or.value() == ScalarOperatorUtil::ConstantOperator) {
        ASSIGN_OR_RETURN(const auto column_name, safe_get_string_member(left, ScalarOperatorUtil::NAME));
        ASSIGN_OR_RETURN(const auto constant_type, safe_get_string_member(right, ScalarOperatorUtil::TYPE));
        if (!right.HasMember(ScalarOperatorUtil::VALUE)) {
            return Status::InternalError("binary predicate right operand missing 'value' field");
        }
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
    if (compound_type == CompoundType_AND || compound_type == CompoundType_OR) {
        if (!children.IsArray() || children.Size() < 2) {
            return Status::InternalError(
                    fmt::format("compound predicate '{}' children must be an array of >=2 elements, got size={}",
                                compound_type, children.IsArray() ? children.Size() : 0));
        }
        ASSIGN_OR_RETURN(const auto left, visitIndexConditionNode(children[0], *this));
        ASSIGN_OR_RETURN(const auto right, visitIndexConditionNode(children[1], *this));
        if (compound_type == CompoundType_AND) {
            ASSIGN_OR_RETURN_PAIMON(auto res, left->And(right));
            return res;
        }
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

StatusOr<std::vector<float>> getQueryVector(const rapidjson::Value& value) {
    if (!value.IsObject() || !value.HasMember(ScalarOperatorUtil::CHILDREN)) {
        return Status::InternalError("ANN query vector argument missing 'children' array");
    }
    const auto& children = value[ScalarOperatorUtil::CHILDREN];
    if (!children.IsArray()) {
        return Status::InternalError("ANN query vector 'children' is not an array");
    }
    std::vector<float> query;
    query.reserve(children.Size());
    for (rapidjson::SizeType i = 0; i < children.Size(); ++i) {
        const auto& element = children[i];
        if (!element.IsObject() || !element.HasMember(ScalarOperatorUtil::VALUE)) {
            return Status::InternalError(fmt::format("ANN query vector element[{}] missing 'value' field", i));
        }
        const auto& v = element[ScalarOperatorUtil::VALUE];
        if (!v.IsNumber()) {
            return Status::InternalError(fmt::format("ANN query vector element[{}].value is not numeric", i));
        }
        query.push_back(v.Get<float>());
    }
    return query;
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexTopNEvaluator::visitCallOperator(
        std::string_view fn_name, const rapidjson::Value& arguments) {
    if (isAnnFunction(fn_name)) {
        if (!arguments.IsArray() || arguments.Size() < 2) {
            return Status::InternalError(
                    fmt::format("ANN function '{}' expects 2+ arguments, got {}", fn_name,
                                arguments.IsArray() ? arguments.Size() : 0));
        }
        ASSIGN_OR_RETURN(const auto column_name, safe_get_string_member(arguments[0], ScalarOperatorUtil::NAME));
        ASSIGN_OR_RETURN(const auto query, getQueryVector(arguments[1]));
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

        // lumina index options. list_size + beam_width come from BE mutable configs so we can
        // sweep them at runtime via ADMIN SET FRONTEND CONFIG without rebuilding. When the
        // requested topN exceeds the configured list_size (FE-side PaimonGlobalIndexBackendSelector
        // currently caps at 1024), upscale linearly so the visitor still returns enough candidates.
        const int32_t cfg_list_size = config::lumina_diskann_search_list_size;
        std::map<std::string, std::string> index_options = {
                {"lumina.diskann.search.list_size", std::to_string(cfg_list_size)},
                {"lumina.search.parallel_number", std::to_string(config::lumina_search_parallel_number)},
                {"lumina.diskann.search.beam_width", std::to_string(config::lumina_diskann_search_beam_width)}};
        if (_n > cfg_list_size) {
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
