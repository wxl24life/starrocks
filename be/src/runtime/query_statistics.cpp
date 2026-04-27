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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/query_statistics.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/query_statistics.h"

namespace starrocks {

void QueryStatistics::to_pb(PQueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    statistics->set_scan_rows(scan_rows);
    statistics->set_scan_bytes(scan_bytes);
    statistics->set_returned_rows(returned_rows);
    statistics->set_cpu_cost_ns(cpu_ns);
    statistics->set_mem_cost_bytes(mem_cost_bytes);
    statistics->set_spill_bytes(spill_bytes);
    statistics->set_transmitted_bytes(transmitted_bytes);
    if (ai_token_usage > 0) {
        statistics->set_ai_token_usage(ai_token_usage);
        statistics->set_ai_prompt_tokens(ai_prompt_tokens);
        statistics->set_ai_completion_tokens(ai_completion_tokens);
        statistics->set_ai_cached_tokens(ai_cached_tokens);
    }
    {
        std::lock_guard l(_lock);
        if (ai_error_rows > 0) {
            statistics->set_ai_error_rows(ai_error_rows);
            if (!ai_first_error.empty()) {
                statistics->set_ai_first_error(ai_first_error);
            }
        }
        for (const auto& [table_id, stats_item] : _stats_items) {
            auto new_stats_item = statistics->add_stats_items();
            new_stats_item->set_table_id(table_id);
            new_stats_item->set_scan_rows(stats_item->scan_rows);
            new_stats_item->set_scan_bytes(stats_item->scan_bytes);
        }
    }

    for (const auto& [node_id, exec_stats_item] : _exec_stats_items) {
        auto new_exec_stats_item = statistics->add_node_exec_stats_items();
        new_exec_stats_item->set_node_id(node_id);
        new_exec_stats_item->set_push_rows(exec_stats_item->push_rows);
        new_exec_stats_item->set_pull_rows(exec_stats_item->pull_rows);
        new_exec_stats_item->set_index_filter_rows(exec_stats_item->index_filter_rows);
        new_exec_stats_item->set_rf_filter_rows(exec_stats_item->rf_filter_rows);
        new_exec_stats_item->set_pred_filter_rows(exec_stats_item->pred_filter_rows);
    }
}

void QueryStatistics::to_params(TAuditStatistics* params) {
    DCHECK(params != nullptr);
    params->__set_scan_rows(scan_rows);
    params->__set_scan_bytes(scan_bytes);
    params->__set_returned_rows(returned_rows);
    params->__set_cpu_cost_ns(cpu_ns);
    params->__set_mem_cost_bytes(mem_cost_bytes);
    params->__set_spill_bytes(spill_bytes);
    params->__set_transmitted_bytes(transmitted_bytes);
    if (ai_token_usage > 0) {
        params->__set_ai_token_usage(ai_token_usage);
        params->__set_ai_prompt_tokens(ai_prompt_tokens);
        params->__set_ai_completion_tokens(ai_completion_tokens);
        params->__set_ai_cached_tokens(ai_cached_tokens);
    }
    {
        std::lock_guard l(_lock);
        if (ai_error_rows > 0) {
            params->__set_ai_error_rows(ai_error_rows);
            if (!ai_first_error.empty()) {
                params->__set_ai_first_error(ai_first_error);
            }
        }
        for (const auto& [table_id, stats_item] : _stats_items) {
            auto new_stats_item = params->stats_items.emplace_back();
            new_stats_item.__set_table_id(table_id);
            new_stats_item.__set_scan_rows(stats_item->scan_rows);
            new_stats_item.__set_scan_bytes(stats_item->scan_bytes);
        }
    }
}

void QueryStatistics::clear() {
    scan_rows = 0;
    scan_bytes = 0;
    cpu_ns = 0;
    returned_rows = 0;
    spill_bytes = 0;
    transmitted_bytes = 0;
    ai_token_usage = 0;
    ai_prompt_tokens = 0;
    ai_completion_tokens = 0;
    ai_cached_tokens = 0;
    ai_error_rows = 0;
    {
        std::lock_guard l(_lock);
        ai_first_error.clear();
    }
    _stats_items.clear();
    _exec_stats_items.clear();
}

void QueryStatistics::update_stats_item(int64_t table_id, int64_t scan_rows, int64_t scan_bytes) {
    if (table_id > 0 && (scan_rows > 0 || scan_bytes > 0)) {
        auto iter = _stats_items.find(table_id);
        if (iter == _stats_items.end()) {
            _stats_items.insert({table_id, std::make_shared<ScanStats>(scan_rows, scan_bytes)});
        } else {
            iter->second->scan_rows += scan_rows;
            iter->second->scan_bytes += scan_bytes;
        }
    }
}

void QueryStatistics::update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                             int64_t index_filter, int64_t rf_filter) {
    auto iter = _exec_stats_items.find(node_id);
    if (iter == _exec_stats_items.end()) {
        _exec_stats_items.insert(
                {node_id, std::make_shared<NodeExecStats>(push, pull, pred_filter, index_filter, rf_filter)});
    } else {
        iter->second->push_rows += push;
        iter->second->pull_rows += pull;
        iter->second->pred_filter_rows += pred_filter;
        iter->second->index_filter_rows += index_filter;
        iter->second->rf_filter_rows += rf_filter;
    }
}

void QueryStatistics::add_stats_item(QueryStatisticsItemPB& stats_item) {
    {
        std::lock_guard l(_lock);
        update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
    }
    this->scan_rows += stats_item.scan_rows();
    this->scan_bytes += stats_item.scan_bytes();
}

void QueryStatistics::add_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred_filter,
                                          int64_t index_filter, int64_t rf_filter) {
    update_exec_stats_item(node_id, push, pull, pred_filter, index_filter, rf_filter);
}

void QueryStatistics::add_scan_stats(int64_t scan_rows, int64_t scan_bytes) {
    this->scan_rows += scan_rows;
    this->scan_bytes += scan_bytes;
}

void QueryStatistics::merge(int sender_id, QueryStatistics& other) {
    // Make the exchange action atomic
    int64_t scan_rows = other.scan_rows.load();
    if (other.scan_rows.compare_exchange_strong(scan_rows, 0)) {
        this->scan_rows += scan_rows;
    }

    int64_t scan_bytes = other.scan_bytes.load();
    if (other.scan_bytes.compare_exchange_strong(scan_bytes, 0)) {
        this->scan_bytes += scan_bytes;
    }

    int64_t cpu_ns = other.cpu_ns.load();
    if (other.cpu_ns.compare_exchange_strong(cpu_ns, 0)) {
        this->cpu_ns += cpu_ns;
        DCHECK(this->cpu_ns >= 0);
    }

    int64_t mem_cost_bytes = other.mem_cost_bytes.load();
    this->mem_cost_bytes = std::max<int64_t>(this->mem_cost_bytes, mem_cost_bytes);

    int64_t spill_bytes = other.spill_bytes.load();
    if (other.spill_bytes.compare_exchange_strong(spill_bytes, 0)) {
        this->spill_bytes += spill_bytes;
    }

    int64_t transmitted_bytes = other.transmitted_bytes.load();
    if (other.transmitted_bytes.compare_exchange_strong(transmitted_bytes, 0)) {
        this->transmitted_bytes += transmitted_bytes;
    }
    int64_t ai_total = other.ai_token_usage.load();
    if (other.ai_token_usage.compare_exchange_strong(ai_total, 0)) {
        this->ai_token_usage += ai_total;
    }
    int64_t ai_prompt = other.ai_prompt_tokens.load();
    if (other.ai_prompt_tokens.compare_exchange_strong(ai_prompt, 0)) {
        this->ai_prompt_tokens += ai_prompt;
    }
    int64_t ai_completion = other.ai_completion_tokens.load();
    if (other.ai_completion_tokens.compare_exchange_strong(ai_completion, 0)) {
        this->ai_completion_tokens += ai_completion;
    }
    int64_t ai_cached = other.ai_cached_tokens.load();
    if (other.ai_cached_tokens.compare_exchange_strong(ai_cached, 0)) {
        this->ai_cached_tokens += ai_cached;
    }
    int64_t ai_errors = other.ai_error_rows.load();
    if (other.ai_error_rows.compare_exchange_strong(ai_errors, 0)) {
        this->ai_error_rows += ai_errors;
        // Lock both to protect ai_first_error on both sides.
        // Consistent address-based ordering prevents deadlock.
        SpinLock* first = &_lock < &other._lock ? &_lock : &other._lock;
        SpinLock* second = &_lock < &other._lock ? &other._lock : &_lock;
        std::lock_guard l1(*first);
        std::lock_guard l2(*second);
        if (this->ai_first_error.empty() && !other.ai_first_error.empty()) {
            this->ai_first_error = std::move(other.ai_first_error);
        }
    }

    {
        std::unordered_map<int64_t, std::shared_ptr<ScanStats>> other_stats_item;
        std::unordered_map<uint32_t, std::shared_ptr<NodeExecStats>> other_exec_stats_items;
        {
            std::lock_guard l(other._lock);
            other_stats_item.swap(other._stats_items);
            other_exec_stats_items.swap(other._exec_stats_items);
        }
        std::lock_guard l(_lock);
        for (const auto& [table_id, stats_item] : other_stats_item) {
            update_stats_item(table_id, stats_item->scan_rows, stats_item->scan_bytes);
        }
        for (const auto& [node_id, exec_stats_item] : other_exec_stats_items) {
            update_exec_stats_item(node_id, exec_stats_item->push_rows, exec_stats_item->pull_rows,
                                   exec_stats_item->pred_filter_rows, exec_stats_item->index_filter_rows,
                                   exec_stats_item->rf_filter_rows);
        }
    }
}

void QueryStatistics::merge_pb(const PQueryStatistics& statistics) {
    if (statistics.has_scan_rows()) {
        scan_rows += statistics.scan_rows();
    }
    if (statistics.has_scan_bytes()) {
        scan_bytes += statistics.scan_bytes();
    }
    if (statistics.has_cpu_cost_ns()) {
        cpu_ns += statistics.cpu_cost_ns();
        DCHECK(cpu_ns >= 0);
    }
    if (statistics.has_spill_bytes()) {
        spill_bytes += statistics.spill_bytes();
    }
    if (statistics.has_ai_token_usage()) {
        ai_token_usage += statistics.ai_token_usage();
    }
    if (statistics.has_ai_prompt_tokens()) {
        ai_prompt_tokens += statistics.ai_prompt_tokens();
    }
    if (statistics.has_ai_completion_tokens()) {
        ai_completion_tokens += statistics.ai_completion_tokens();
    }
    if (statistics.has_ai_cached_tokens()) {
        ai_cached_tokens += statistics.ai_cached_tokens();
    }
    if (statistics.has_ai_error_rows()) {
        ai_error_rows += statistics.ai_error_rows();
    }
    if (statistics.has_ai_first_error()) {
        std::lock_guard l(_lock);
        if (ai_first_error.empty()) {
            ai_first_error = statistics.ai_first_error();
        }
    }
    if (statistics.has_mem_cost_bytes()) {
        mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
    }
    if (statistics.has_transmitted_bytes()) {
        transmitted_bytes += statistics.transmitted_bytes();
    }
    {
        std::lock_guard l(_lock);
        for (int i = 0; i < statistics.stats_items_size(); ++i) {
            const auto& stats_item = statistics.stats_items(i);
            update_stats_item(stats_item.table_id(), stats_item.scan_rows(), stats_item.scan_bytes());
        }
        for (int i = 0; i < statistics.node_exec_stats_items_size(); ++i) {
            const auto& exec_stats_item = statistics.node_exec_stats_items(i);
            update_exec_stats_item(exec_stats_item.node_id(), exec_stats_item.push_rows(), exec_stats_item.pull_rows(),
                                   exec_stats_item.pred_filter_rows(), exec_stats_item.index_filter_rows(),
                                   exec_stats_item.rf_filter_rows());
        }
    }
}

void QueryStatisticsRecvr::insert(const PQueryStatistics& statistics, int sender_id) {
    std::lock_guard<SpinLock> l(_lock);
    QueryStatistics* query_statistics = nullptr;
    auto iter = _query_statistics.find(sender_id);
    if (iter == _query_statistics.end()) {
        query_statistics = new QueryStatistics;
        _query_statistics[sender_id] = query_statistics;
    } else {
        query_statistics = iter->second;
    }
    query_statistics->merge_pb(statistics);
}

void QueryStatisticsRecvr::aggregate(QueryStatistics* statistics) {
    std::lock_guard<SpinLock> l(_lock);
    for (auto& pair : _query_statistics) {
        statistics->merge(pair.first, *pair.second);
    }
}

QueryStatisticsRecvr::~QueryStatisticsRecvr() {
    // It is unnecessary to lock here, because the destructor will be
    // called alter DataStreamRecvr's close in ExchangeNode.
    for (auto& pair : _query_statistics) {
        delete pair.second;
    }
    _query_statistics.clear();
}

} // namespace starrocks
