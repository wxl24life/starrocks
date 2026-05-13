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
import com.aliyun.odps.Schema;
import com.aliyun.odps.Schemas;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OdpsMetadataTest extends MockedBase {

    @Mock
    protected static OdpsMetadata odpsMetadata;

    @BeforeAll
    public static void setUp() throws IOException, ExecutionException, OdpsException {
        initMock();
        odpsMetadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, odpsProperties);
    }

    @Test
    public void testInitMeta() {
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, "project");
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.ENABLE_PARTITION_CACHE, "false");
        properties.put(OdpsProperties.ENABLE_TABLE_CACHE, "false");
        properties.put(OdpsProperties.ENABLE_TABLE_NAME_CACHE, "true");
        OdpsMetadata metadata = new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(properties));
        Assertions.assertNotNull(metadata);
    }

    @Test
    public void testListDbNames() {
        List<String> expectedDbNames = Collections.singletonList("project");
        List<String> dbNames = odpsMetadata.listDbNames(new ConnectContext());
        Assertions.assertEquals(dbNames, expectedDbNames);
    }

    @Test
    public void testGetDb() {
        Database database = odpsMetadata.getDb(new ConnectContext(), "project");
        Assertions.assertNotNull(database);
        Assertions.assertEquals(database.getFullName(), "project");
    }

    @Test
    public void testListTableNames() {
        List<String> project = odpsMetadata.listTableNames(new ConnectContext(), "project");
        Assertions.assertEquals(Collections.singletonList("tableName"), project);
    }

    @Test
    public void testGetTable() throws ExecutionException {
        OdpsTable table = (OdpsTable) odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(table.isOdpsTable());
        Assertions.assertEquals("tableName", table.getName());
        Assertions.assertEquals("project", table.getCatalogDBName());
        Assertions.assertFalse(table.isUnPartitioned());
        Assertions.assertEquals("c1", table.getColumn("c1").getName());
    }

    @Test
    public void testListPartitionNames() {
        List<String> partitionNames =
                odpsMetadata.listPartitionNames("project", "tableName", ConnectorMetadatRequestContext.DEFAULT);
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitionNames);
    }

    @Test
    public void testListPartitionNamesByValue() {
        List<String> partitions = odpsMetadata.listPartitionNamesByValue("project", "tableName",
                ImmutableList.of(Optional.of("a"), Optional.empty()));
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitions);

        partitions = odpsMetadata.listPartitionNamesByValue("project", "tableName",
                ImmutableList.of(Optional.empty(), Optional.of("b")));
        Assertions.assertEquals(Collections.singletonList("p1=a/p2=b"), partitions);
    }

    @Test
    public void testGetPartitions() {
        Table table = odpsMetadata.getTable(new ConnectContext(), "db", "tbl");
        List<String> partitionNames = odpsMetadata.listPartitionNames("db", "tbl", ConnectorMetadatRequestContext.DEFAULT);
        List<PartitionInfo> partitions = odpsMetadata.getPartitions(table, partitionNames);
        Assertions.assertEquals(1, partitions.size());
        PartitionInfo partitionInfo = partitions.get(0);
        Assertions.assertTrue(partitionInfo.getModifiedTime() > 0);
    }

    @Test
    public void testRefreshTable() {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        // mock schema change
        when(table.getSchema()).thenReturn(new TableSchema());

        Table cacheTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(cacheTable.getColumns().size() > 0);

        odpsMetadata.refreshTable("project", odpsTable, null, false);
        Table refreshTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        Assertions.assertTrue(refreshTable.getColumns().size() == 0);
    }

    @Test
    public void testGetRemoteFiles() throws AnalysisException, IOException {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey =
                PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                        odpsTable.getPartitionColumns());
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey)).build();
        List<RemoteFileInfo> remoteFileInfos =
                odpsMetadata.getRemoteFiles(odpsTable, params, mockTableReadSessionBuilder);
        Assertions.assertEquals(1, remoteFileInfos.size());
    }

    @Test
    public void testGetCloudConfiguration() {
        AliyunCloudConfiguration cloudConfiguration = (AliyunCloudConfiguration) odpsMetadata.getCloudConfiguration();
        Assertions.assertEquals(CloudType.ALIYUN, cloudConfiguration.getCloudType());
        Assertions.assertEquals("ak", cloudConfiguration.getAliyunCloudCredential().getAccessKey());
        Assertions.assertEquals("sk", cloudConfiguration.getAliyunCloudCredential().getSecretKey());
        Assertions.assertEquals("http://127.0.0.1", cloudConfiguration.getAliyunCloudCredential().getEndpoint());
    }

    @Test
    public void testOdpsTableToThrift() {
        OdpsTable odpsTable = new OdpsTable("catalog", table);
        TTableDescriptor thrift = odpsTable.toThrift(null);
        Assertions.assertNotNull(thrift);
    }

    private OdpsMetadata newSchemaModeMetadata() {
        Map<String, String> p = new HashMap<>();
        p.put(OdpsProperties.ACCESS_ID, "ak");
        p.put(OdpsProperties.ACCESS_KEY, "sk");
        p.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        p.put(OdpsProperties.PROJECT, "project");
        p.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        p.put(OdpsProperties.ENABLE_NAMESPACE_SCHEMA, "true");
        p.put(OdpsProperties.ENABLE_TABLE_CACHE, "false");
        p.put(OdpsProperties.ENABLE_PARTITION_CACHE, "false");
        p.put(OdpsProperties.ENABLE_TABLE_NAME_CACHE, "false");
        p.put(OdpsProperties.ENABLE_PREDICATE_PUSHDOWN, "false");
        return new OdpsMetadata(odps, "odps", aliyunCloudCredential, new OdpsProperties(p));
    }

    @Test
    public void testSchemaModeListDbNames() {
        Schemas schemas = Mockito.mock(Schemas.class);
        Schema sch1 = Mockito.mock(Schema.class);
        Schema sch2 = Mockito.mock(Schema.class);
        when(sch1.getName()).thenReturn("sch1");
        when(sch2.getName()).thenReturn("sch2");
        Iterator<Schema> it = Arrays.asList(sch1, sch2).iterator();
        when(odps.schemas()).thenReturn(schemas);
        when(schemas.iterator(eq("project"))).thenReturn(it);

        OdpsMetadata schemaMetadata = newSchemaModeMetadata();
        List<String> names = schemaMetadata.listDbNames(new ConnectContext());
        Assertions.assertEquals(Arrays.asList("sch1", "sch2"), names);
    }

    @Test
    public void testSchemaModeGetTableUses3ArgSdkCall() {
        when(tables.get(eq("project"), eq("sch1"), eq("tableName"))).thenReturn(table);

        OdpsMetadata schemaMetadata = newSchemaModeMetadata();
        OdpsTable t = (OdpsTable) schemaMetadata.getTable(new ConnectContext(), "sch1", "tableName");
        Assertions.assertNotNull(t);
        Assertions.assertEquals("project", t.getProjectName());
        Assertions.assertEquals("sch1", t.getSchemaName());
        Assertions.assertEquals("sch1", t.getCatalogDBName());
        verify(tables, atLeastOnce()).get(eq("project"), eq("sch1"), eq("tableName"));
    }

    @Test
    public void testSchemaModeListTableNamesUses4ArgIterator() {
        Iterator<com.aliyun.odps.Table> localIt = Collections.singletonList(table).iterator();
        when(tables.iterator(eq("project"), eq("sch1"), eq(null), eq(false))).thenReturn(localIt);

        OdpsMetadata schemaMetadata = newSchemaModeMetadata();
        List<String> tbls = schemaMetadata.listTableNames(new ConnectContext(), "sch1");
        Assertions.assertEquals(Collections.singletonList("tableName"), tbls);
        verify(tables, atLeastOnce()).iterator(eq("project"), eq("sch1"), eq(null), eq(false));
    }

    @Test
    public void testSchemaModeGetRemoteFilesUses3ArgIdentifier() throws IOException {
        when(tables.get(eq("project"), eq("sch1"), eq("tableName"))).thenReturn(table);

        OdpsMetadata schemaMetadata = newSchemaModeMetadata();
        OdpsTable t = (OdpsTable) schemaMetadata.getTable(new ConnectContext(), "sch1", "tableName");
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(t.getPartitionColumnNames())
                .setPartitionKeys(Collections.emptyList())
                .build();
        ArgumentCaptor<TableIdentifier> captor = ArgumentCaptor.forClass(TableIdentifier.class);
        when(mockTableReadSessionBuilder.identifier(captor.capture())).thenReturn(mockTableReadSessionBuilder);

        schemaMetadata.getRemoteFiles(t, params, mockTableReadSessionBuilder);
        TableIdentifier captured = captor.getValue();
        Assertions.assertEquals("project", captured.getProject());
        Assertions.assertEquals("sch1", captured.getSchema());
        Assertions.assertEquals("tableName", captured.getTable());
    }

    @Test
    public void testOdpsTableToThriftAlwaysCarriesProject() {
        OdpsTable t = new OdpsTable("catalog", "project", "sch1", table);
        TTableDescriptor thrift = t.toThrift(null);
        Assertions.assertEquals("project", thrift.getDbName());
        Assertions.assertEquals("sch1", t.getCatalogDBName());
    }

    @Test
    public void testOdpsTableSchemaUUIDUnique() {
        OdpsTable a = new OdpsTable("catalog", "project", "sch1", table);
        OdpsTable b = new OdpsTable("catalog", "project", "sch2", table);
        Assertions.assertNotEquals(a.getUUID(), b.getUUID());
    }

    @Test
    public void testGetRemoteFilesWithSmallLimit() throws AnalysisException, IOException {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                odpsTable.getPartitionColumns());

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey))
                .setLimit(100L)
                .build();

        List<RemoteFileInfo> remoteFileInfos = odpsMetadata.getRemoteFiles(odpsTable, params, mockTableReadSessionBuilder);
        Assertions.assertEquals(1, remoteFileInfos.size());
    }

    @Test
    public void testGetRemoteFilesWithLargeLimit() throws AnalysisException, IOException {
        Table odpsTable = odpsMetadata.getTable(new ConnectContext(), "project", "tableName");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue("a"), new PartitionValue("b")),
                odpsTable.getPartitionColumns());

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setFieldNames(odpsTable.getPartitionColumnNames())
                .setPartitionKeys(ImmutableList.of(partitionKey))
                .setLimit(20000L)
                .build();

        List<RemoteFileInfo> remoteFileInfos = odpsMetadata.getRemoteFiles(odpsTable, params, mockTableReadSessionBuilder);
        Assertions.assertEquals(1, remoteFileInfos.size());
    }
}

