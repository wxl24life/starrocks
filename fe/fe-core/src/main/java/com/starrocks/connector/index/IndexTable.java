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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.index.TableIndexMetadata.INDEX_DB_NAME;

/**
 * IndexTable is a logical table that represents the index table of a physical table.
 */
public class IndexTable extends Table {

    public static final String INDEX_TABLE_SUFFIX = "$global_index";

    public static final String INDEX_RESULT_COLUMN_NAME = "index_result";
    public static final String ARGS_COLUMN_NAME = "args";

    private static final Column INDEX_RESULT_COLUMN = new Column(INDEX_RESULT_COLUMN_NAME, Type.VARBINARY, true);
    private static final Column ARGS_COLUMN = new Column(ARGS_COLUMN_NAME, Type.VARCHAR, true);

    protected Table table;

    public IndexTable() {
        super(TableType.INDEX);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TableType getInnerType() {
        return table.getType();
    }

    public Table getInnerTable() {
        return table;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getFullSchema() {
        return Arrays.asList(INDEX_RESULT_COLUMN, ARGS_COLUMN);
    }

    @Override
    public Map<ColumnId, Column> getIdToColumn() {
        return Map.of(
                INDEX_RESULT_COLUMN.getColumnId(), INDEX_RESULT_COLUMN,
                ARGS_COLUMN.getColumnId(), ARGS_COLUMN
        );
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(getId(), TTableType.INDEX_TABLE, 2, 0, getName(), INDEX_DB_NAME);
    }

    @Override
    public String getName() {
        if (table == null) {
            throw new IllegalStateException("IndexTable.setTable must be called before getName");
        }
        return table.getName() + INDEX_TABLE_SUFFIX;
    }

}
