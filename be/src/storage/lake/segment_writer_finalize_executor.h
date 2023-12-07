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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memtable_flush_executor.h

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

#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "general_tablet_writer.h"
#include "util/threadpool.h"

namespace starrocks::lake {

class VerticalGeneralTabletWriter;

class SegmentWriterFinalizeToken {
public:
    SegmentWriterFinalizeToken(std::unique_ptr<ThreadPoolToken> thread_pool_token);

    ThreadPoolToken* get_thread_pool_token() { return _thread_pool_token.get(); }

    void cancel();

    void wait();

private:
    std::unique_ptr<ThreadPoolToken> _thread_pool_token;
    std::shared_ptr<VerticalGeneralTabletWriter> _tablet_writer;
};

class SegmentWriterFinalizeExecutor {
public:
    SegmentWriterFinalizeExecutor() = default;
    ~SegmentWriterFinalizeExecutor() = default;

    Status init();

    Status update_max_threads(int max_threads);

    std::unique_ptr<SegmentWriterFinalizeToken> create_finalize_token(
            ThreadPool::ExecutionMode execution_mode = ThreadPool::ExecutionMode::SERIAL);

    ThreadPool* get_thread_pool() { return _finalize_thread_pool.get(); }

private:
    std::unique_ptr<ThreadPool> _finalize_thread_pool;
};

} // namespace starrocks::lake
