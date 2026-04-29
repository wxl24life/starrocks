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

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class LogicalPaimonIndexScanOperator extends LogicalScanOperator {

    public LogicalPaimonIndexScanOperator(
            IndexTable indexTable,
            ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            long limit
    ) {
        super(OperatorType.LOGICAL_PAIMON_INDEX_SCAN,
                indexTable,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                null,
                null);
    }

    private LogicalPaimonIndexScanOperator() {
        super(OperatorType.LOGICAL_PAIMON_INDEX_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalPaimonIndexScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalPaimonIndexScanOperator, LogicalPaimonIndexScanOperator.Builder> {

        @Override
        protected LogicalPaimonIndexScanOperator newInstance() {
            return new LogicalPaimonIndexScanOperator();
        }

        @Override
        public LogicalPaimonIndexScanOperator.Builder withOperator(LogicalPaimonIndexScanOperator scanOperator) {
            super.withOperator(scanOperator);
            return this;
        }
    }

}
