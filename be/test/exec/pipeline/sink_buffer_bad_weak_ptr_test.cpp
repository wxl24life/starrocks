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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "util/disposable_closure.h"

namespace starrocks::pipeline {

TEST(SinkBufferBadWeakPtrTest, weak_from_this_returns_nullptr_after_query_ctx_destroyed) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    TUniqueId query_id;
    query_id.hi = 999;
    query_id.lo = 1;

    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    auto weak = query_ctx->weak_from_this();
    ASSERT_NE(weak.lock(), nullptr);

    query_ctx->count_down_fragments();
    ASSERT_TRUE(query_ctx->is_dead());
    query_ctx_mgr->remove(query_id);
    ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);

    ASSERT_EQ(weak.lock(), nullptr);
}

TEST(SinkBufferBadWeakPtrTest, shared_from_this_throws_after_query_ctx_destroyed) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    TUniqueId query_id;
    query_id.hi = 999;
    query_id.lo = 2;

    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    auto weak = query_ctx->weak_from_this();
    ASSERT_NO_THROW(query_ctx->shared_from_this());

    query_ctx->count_down_fragments();
    query_ctx_mgr->remove(query_id);

    ASSERT_EQ(weak.lock(), nullptr);
}

TEST(SinkBufferBadWeakPtrTest, simulate_closure_callback_after_query_ctx_destroyed) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    TUniqueId query_id;
    query_id.hi = 999;
    query_id.lo = 3;

    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    std::atomic<bool> is_finishing{false};
    std::atomic<int32_t> total_in_flight_rpc{1};
    std::atomic<int32_t> num_finished_rpcs{0};
    std::atomic<int32_t> num_in_flight_rpcs{1};

    auto query_ctx_weak = query_ctx->weak_from_this();

    auto callback = [&query_ctx_weak, &is_finishing, &total_in_flight_rpc, &num_finished_rpcs,
                     &num_in_flight_rpcs]() {
        auto query_ctx_guard = query_ctx_weak.lock();
        if (!query_ctx_guard) {
            is_finishing = true;
            ++num_finished_rpcs;
            --num_in_flight_rpcs;
            --total_in_flight_rpc;
            return;
        }
        ++num_finished_rpcs;
        --num_in_flight_rpcs;
        --total_in_flight_rpc;
    };

    query_ctx->count_down_fragments();
    query_ctx_mgr->remove(query_id);

    ASSERT_NO_FATAL_FAILURE(callback());

    ASSERT_TRUE(is_finishing.load());
    ASSERT_EQ(total_in_flight_rpc.load(), 0);
    ASSERT_EQ(num_finished_rpcs.load(), 1);
    ASSERT_EQ(num_in_flight_rpcs.load(), 0);
}

TEST(SinkBufferBadWeakPtrTest, simulate_concurrent_callback_and_destroy) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    for (int iter = 0; iter < 100; ++iter) {
        TUniqueId query_id;
        query_id.hi = 1000 + iter;
        query_id.lo = 4;

        ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->set_total_fragments(1);
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        auto query_ctx_weak = query_ctx->weak_from_this();

        std::atomic<bool> callback_entered{false};
        std::atomic<bool> callback_done{false};
        std::atomic<bool> destroy_done{false};

        std::thread callback_thread([&]() {
            callback_entered = true;
            while (!destroy_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            auto guard = query_ctx_weak.lock();
            if (guard) {
                ASSERT_NE(guard, nullptr);
            }
            callback_done = true;
        });

        while (!callback_entered.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        query_ctx->count_down_fragments();
        query_ctx_mgr->remove(query_id);
        destroy_done.store(true, std::memory_order_release);

        callback_thread.join();

        ASSERT_TRUE(callback_done.load());
    }
}

TEST(SinkBufferBadWeakPtrTest, cancelled_by_fe_accelerates_destroy) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    TUniqueId query_id;
    query_id.hi = 999;
    query_id.lo = 5;

    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(3);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    auto query_ctx_weak = query_ctx->weak_from_this();

    query_ctx->cancel(Status::Cancelled("cancelled"), true);

    ASSERT_TRUE(query_ctx->is_delivery_expired());

    query_ctx->count_down_fragments();
    ASSERT_TRUE(query_ctx->is_dead());

    query_ctx_mgr->remove(query_id);
    ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);

    auto guard = query_ctx_weak.lock();
    ASSERT_EQ(guard, nullptr);
}

TEST(SinkBufferBadWeakPtrTest, reproduce_shared_from_this_crash_after_destroy) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    TUniqueId query_id;
    query_id.hi = 999;
    query_id.lo = 6;

    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    auto query_ctx_weak = query_ctx->weak_from_this();

    query_ctx->count_down_fragments();
    query_ctx_mgr->remove(query_id);

    ASSERT_EQ(query_ctx_weak.lock(), nullptr);
    ASSERT_THROW(
            {
                auto guard = query_ctx_weak.lock();
                if (!guard) {
                    throw std::bad_weak_ptr();
                }
                guard->shared_from_this();
            },
            std::bad_weak_ptr);
}

} // namespace starrocks::pipeline
