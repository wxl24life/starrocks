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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memtable_flush_executor.cpp

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

#include "segment_writer_finalize_executor.h"

#include <memory>
#include <utility>

namespace starrocks::lake {
SegmentWriterFinalizeToken::SegmentWriterFinalizeToken(std::unique_ptr<ThreadPoolToken> thread_pool_token)
        : _thread_pool_token(std::move(thread_pool_token)) {}

void SegmentWriterFinalizeToken::cancel() {
    _thread_pool_token->shutdown();
}

void SegmentWriterFinalizeToken::wait() {
    _thread_pool_token->wait();
}

Status SegmentWriterFinalizeExecutor::init() {
    return ThreadPoolBuilder("segment_writer_finalize")
            .set_min_threads(1)
            .set_max_threads(config::lake_async_segment_writer_thread_number)
            .set_max_queue_size(1000)
            .build(&_finalize_thread_pool);
}

Status SegmentWriterFinalizeExecutor::update_max_threads(int max_threads) {
    if (_finalize_thread_pool != nullptr) {
        return _finalize_thread_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::unique_ptr<SegmentWriterFinalizeToken> SegmentWriterFinalizeExecutor::create_finalize_token(
        ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<SegmentWriterFinalizeToken>(_finalize_thread_pool->new_token(execution_mode));
}

} // namespace starrocks::lake
