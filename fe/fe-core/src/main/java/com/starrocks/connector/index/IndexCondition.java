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

package com.starrocks.connector.index;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

public class IndexCondition {

    protected ScalarOperator predicate;

    public IndexCondition() {}

    public IndexCondition(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    // LinkedHashMap (not HashMap) so the JSON sent to BE has deterministic key order, which makes
    // trace logs easier to diff.
    public Map<String, Object> toQueryJson() {
        Map<String, Object> queryJson = new LinkedHashMap<>();
        queryJson.put("predicate", ScalarOperatorSerializer.toJson(predicate));
        return queryJson;
    }

    public Map<String, Object> toDebugJson() {
        Map<String, Object> debugJson = new LinkedHashMap<>();
        debugJson.put("predicate", predicate == null ? "null" : ScalarOperatorSerializer.toSql(predicate));
        return debugJson;
    }

    public String toDebugString() {
        return new org.json.JSONObject(toDebugJson()).toString();
    }
}
