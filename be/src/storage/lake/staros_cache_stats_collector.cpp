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

#ifdef USE_STAROS

#include "storage/lake/staros_cache_stats_collector.h"

#include <fslib/cache_stats_collector.h>
#include <fslib/configuration.h>

#include "fs/fs_starlet.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

using Configuration = staros::starlet::fslib::Configuration;

size_t calculate_cache_size(std::vector<std::string> paths) {
    if (paths.empty()) {
        return Status::OK();
    }

    // REQUIRE: All files in |paths| have the same file system scheme.
    ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(paths[0]));
    Configuration conf;
    auto fs_st = g_worker->get_shard_filesystem(pair.second, conf);
    if (!fs_st.ok()) {
        LOG(WARNING) << "Invalid statlet file path: " + file_path);
        return 0;
    }

    CacheStatCollector* collector = CacheStatCollector::instance(fs_st.get());
    StatusOr<size_t> size_st = collector->collect_cache_size(paths);
    if (size_st.ok()) {
        return size_st.get();
    }
    return 0;
}
} // namespace starrocks::lake
#endif