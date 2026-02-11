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

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.odps.OdpsConnectorScanRangeSource;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * full scan on ODPS table.
 */
public class OdpsScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OdpsScanNode.class);
    private OdpsTable table;
    private CloudConfiguration cloudConfiguration = null;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private OdpsConnectorScanRangeSource scanRangeSource;

    public OdpsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (OdpsTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    private void setupCloudCredential() {
        String catalog = table.getCatalogName();
        if (catalog == null) {
            return;
        }
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalog));
        cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalog));
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate,
                                        List<PartitionKey> partitionKeys, long limit) {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setPartitionKeys(partitionKeys).setPredicate(predicate)
                .setFieldNames(fieldNames).setLimit(limit).build();

        RemoteFileInfoSource remoteFileInfoSource =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(table, params);

        scanRangeSource = new OdpsConnectorScanRangeSource(table, remoteFileInfoSource, tupleDescriptor);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (scanRangeSource == null) {
            return new ArrayList<>();
        }
        return scanRangeSource.getAllOutputs();
    }

    public OdpsConnectorScanRangeSource getScanRangeSource() {
        return scanRangeSource;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("odpsTable=" + table.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getCatalogDBName()).append(".").append(table.getCatalogTableName())
                .append("\n");
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());

        String explainString = getExplainString(conjuncts);
        LOG.info("Explain string: " + explainString);
        tHdfsScanNode.setSql_predicates(explainString);

        if (table != null) {
            tHdfsScanNode.setTable_name(table.getCatalogTableName());
        }
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tCloudConfiguration.setCloud_type(TCloudType.ALIYUN);
        tHdfsScanNode.setCloud_configuration(tCloudConfiguration);
        msg.hdfs_scan_node = tHdfsScanNode;
    }
}
