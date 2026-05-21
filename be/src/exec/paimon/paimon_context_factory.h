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

#include <memory>
#include <string>
#include <vector>

#include <paimon/memory/memory_pool.h>
#include <paimon/read_context.h>
#include <paimon/result.h>
#include "exec/hdfs_scanner.h"

namespace starrocks {

class PaimonFileSystem;

class PaimonContextFactory {
public:
    struct ReadOptions {
        const HdfsScannerParams& scanner_params;
        const HdfsScannerContext& scanner_ctx;
        std::vector<std::string> field_names;
        std::shared_ptr<paimon::MemoryPool> memory_pool;
        std::shared_ptr<PaimonFileSystem> paimon_fs;
        int read_batch_size = 0;
    };

    static paimon::Result<std::unique_ptr<paimon::ReadContext>> build_read_context(const ReadOptions& options);

private:
    static void _apply_read_options(paimon::ReadContextBuilder* builder, const ReadOptions& options);

    static void _add_read_predicate(paimon::ReadContextBuilder* builder, const HdfsScannerParams& scanner_params,
                                    const HdfsScannerContext& scanner_ctx);
};

} // namespace starrocks
