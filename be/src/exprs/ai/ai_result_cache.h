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

#include <string>

namespace starrocks {

class Cache;

// Global LRU result cache for AI function responses.
// Key: post_data (full JSON request body, unique per model + prompt + params).
// Thread-safe (backed by ShardedLRUCache).
class AIResultCache {
public:
    static AIResultCache* instance();

    // Chat result: returns true on hit.
    bool lookup(const std::string& cache_key, std::string* result);
    void insert(const std::string& cache_key, const std::string& result);

    void shutdown();

private:
    AIResultCache();
    ~AIResultCache();

    Cache* _cache = nullptr;
};

} // namespace starrocks
