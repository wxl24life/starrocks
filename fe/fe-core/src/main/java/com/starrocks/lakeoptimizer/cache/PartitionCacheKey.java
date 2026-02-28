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

package com.starrocks.lakeoptimizer.cache;

import java.util.Objects;

/**
 * Cache key for partition metadata.
 */
public class PartitionCacheKey {
    public final long tableId;
    public final long snapshotId;

    public PartitionCacheKey(long tableId, long snapshotId) {
        this.tableId = tableId;
        this.snapshotId = snapshotId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionCacheKey that = (PartitionCacheKey) o;
        return tableId == that.tableId && snapshotId == that.snapshotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, snapshotId);
    }

    @Override
    public String toString() {
        return "PartitionCacheKey{tableId=" + tableId + ", snapshotId=" + snapshotId + "}";
    }
}

