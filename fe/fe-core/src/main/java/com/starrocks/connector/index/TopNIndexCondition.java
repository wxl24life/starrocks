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

import java.util.Arrays;
import java.util.Map;

public class TopNIndexCondition extends IndexCondition {

    private ScalarOperator scoreExpr;
    private long[] nLocal;

    public ScalarOperator getScoreExpr() {
        return scoreExpr;
    }

    public void setScoreExpr(ScalarOperator scoreExpr) {
        this.scoreExpr = scoreExpr;
    }

    public long[] getNLocal() {
        return nLocal;
    }

    public void setNLocal(double[] nLocal) {
        this.nLocal = new long[nLocal.length];
        for (int i = 0; i < nLocal.length; i++) {
            this.nLocal[i] = (long) nLocal[i];
        }
    }

    @Override
    public Map<String, Object> toQueryJson() {
        Map<String, Object> queryJson = super.toQueryJson();
        queryJson.put("scoreExpr", ScalarOperatorSerializer.toJson(scoreExpr));
        queryJson.put("n", nLocal);
        return queryJson;
    }

    @Override
    public Map<String, Object> toDebugJson() {
        Map<String, Object> debugJson = super.toDebugJson();
        debugJson.put("scoreExpr", scoreExpr == null ? "null" : ScalarOperatorSerializer.toSql(scoreExpr));
        debugJson.put("n", Arrays.toString(nLocal));
        return debugJson;
    }
}
