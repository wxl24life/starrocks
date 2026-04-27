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

#include "exprs/ai/ai_result_cache.h"

#include <cstring>

#include "common/config.h"
#include "common/logging.h"
#include "util/lru_cache.h"

namespace starrocks {

static constexpr size_t kDefaultCacheCapacityBytes = 128ULL * 1024 * 1024; // 128MB

struct CachedString {
    size_t len;
    char data[];
};

static void delete_cached_string(const CacheKey& /*key*/, void* value) {
    ::free(value);
}

AIResultCache::AIResultCache() {
    int32_t mb = config::ai_function_result_cache_mb;
    size_t capacity = (mb > 0) ? static_cast<size_t>(mb) * 1024 * 1024 : kDefaultCacheCapacityBytes;
    _cache = new_lru_cache(capacity);
}

AIResultCache::~AIResultCache() {
    delete _cache;
    _cache = nullptr;
}

AIResultCache* AIResultCache::instance() {
    static AIResultCache s_instance;
    return &s_instance;
}

bool AIResultCache::lookup(const std::string& cache_key, std::string* result) {
    if (!config::ai_function_result_cache_enabled || _cache == nullptr) return false;

    auto* handle = _cache->lookup(cache_key);
    if (handle == nullptr) return false;

    auto* entry = reinterpret_cast<CachedString*>(_cache->value(handle));
    result->assign(entry->data, entry->len);
    _cache->release(handle);
    return true;
}

void AIResultCache::insert(const std::string& cache_key, const std::string& result) {
    if (!config::ai_function_result_cache_enabled || _cache == nullptr) return;
    if (result.size() > SIZE_MAX - sizeof(CachedString)) return;

    size_t alloc_size = sizeof(CachedString) + result.size();
    auto* entry = reinterpret_cast<CachedString*>(::malloc(alloc_size));
    if (entry == nullptr) return;
    entry->len = result.size();
    std::memcpy(entry->data, result.data(), result.size());

    auto* handle = _cache->insert(cache_key, entry, alloc_size, alloc_size, &delete_cached_string);
    if (handle != nullptr) {
        _cache->release(handle);
    } else {
        VLOG(1) << "[AI] cache insert failed, key_size=" << cache_key.size()
                << " value_size=" << result.size();
        ::free(entry);
    }
}

void AIResultCache::shutdown() {
    if (_cache != nullptr) {
        _cache->prune();
    }
}

} // namespace starrocks
