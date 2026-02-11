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

package com.starrocks.connector.odps;

import com.aliyun.odps.OdpsException;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class OdpsAsyncRemoteInfoSourceTest extends MockedBase {

    @BeforeAll
    public static void setUp() throws IOException, ExecutionException, OdpsException {
        initMock();
    }

    @Test
    public void testOdpsAsyncRemoteInfoSource() throws Exception {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                odpsTable.getPartitionColumns());

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey))
                .build();

        OdpsAsyncRemoteInfoSource asyncSource = new OdpsAsyncRemoteInfoSource(
                Executors.newFixedThreadPool(1),
                odpsTable,
                params,
                odpsMetadata);

        asyncSource.run();
        RemoteFileInfo result = asyncSource.getOutput();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFiles().size());
    }

    @Test
    public void testOdpsConnectorScanRangeSource() throws Exception {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                odpsTable.getPartitionColumns());

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey))
                .build();

        OdpsAsyncRemoteInfoSource asyncSource = new OdpsAsyncRemoteInfoSource(
                Executors.newFixedThreadPool(1),
                odpsTable,
                params,
                odpsMetadata);

        asyncSource.run();
        Thread.sleep(1000);

        OdpsConnectorScanRangeSource scanRangeSource = new OdpsConnectorScanRangeSource(
                (OdpsTable) odpsTable,
                asyncSource,
                null);

        List<com.starrocks.thrift.TScanRangeLocations> scanRanges = scanRangeSource.getSourceOutputs(10);
        Assertions.assertNotNull(scanRanges);
        Assertions.assertEquals(2, scanRanges.size());
    }
}