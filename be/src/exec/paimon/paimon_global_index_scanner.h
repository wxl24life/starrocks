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

#include <gen_cpp/CloudConfiguration_types.h>
#include <paimon/global_index/global_index_result.h>

#include "exec/hdfs_scanner.h"

namespace starrocks {

class PaimonGlobalIndexScanner final : public HdfsScanner {
public:
    explicit PaimonGlobalIndexScanner(const TCloudConfiguration& cloud_conf) : _cloud_conf(cloud_conf) {}
    ~PaimonGlobalIndexScanner() override = default;
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

private:
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> evaluateGlobalIndex() const;
    int32_t return_rows = 0;

    // Cloud credentials forwarded from THdfsScanNode.cloud_configuration. Stored as a copy so
    // that ::evaluateGlobalIndex (called from do_get_next, after the FE-side stack has gone)
    // can still read the OSS keys when constructing the paimon::FileSystem.
    TCloudConfiguration _cloud_conf;
};

} // namespace starrocks
