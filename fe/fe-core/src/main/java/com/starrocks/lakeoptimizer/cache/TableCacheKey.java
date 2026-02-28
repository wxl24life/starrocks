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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Cache key for Table in LakeOptimizer.
 * Also serves as the edit log entry for cross-FE cache invalidation.
 */
public class TableCacheKey implements Writable {
    @SerializedName("catalogName")
    public final String catalogName;

    @SerializedName("dbName")
    public final String dbName;

    @SerializedName("tableName")
    public final String tableName;

    public TableCacheKey(String catalogName, String dbName, String tableName) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableCacheKey read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableCacheKey.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableCacheKey that = (TableCacheKey) o;
        return Objects.equals(catalogName, that.catalogName) &&
               Objects.equals(dbName, that.dbName) &&
               Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, dbName, tableName);
    }

    @Override
    public String toString() {
        return catalogName + "." + dbName + "." + tableName;
    }
}
