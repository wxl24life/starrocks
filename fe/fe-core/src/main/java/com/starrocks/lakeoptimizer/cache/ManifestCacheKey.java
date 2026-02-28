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
 * Cache key for manifest entry metadata.
 */
public class ManifestCacheKey {
    public final long tableId;
    public final long snapshotId;
    public final String partitionName;
    public final int bucket;

    public ManifestCacheKey(long tableId, long snapshotId, String partitionName, int bucket) {
        this.tableId = tableId;
        this.snapshotId = snapshotId;
        this.partitionName = partitionName;
        this.bucket = bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestCacheKey that = (ManifestCacheKey) o;
        return tableId == that.tableId 
                && snapshotId == that.snapshotId 
                && bucket == that.bucket
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, snapshotId, partitionName, bucket);
    }

    @Override
    public String toString() {
        return "ManifestCacheKey{tableId=" + tableId + ", snapshotId=" + snapshotId 
                + ", partition=" + partitionName + ", bucket=" + bucket + "}";
    }
}

