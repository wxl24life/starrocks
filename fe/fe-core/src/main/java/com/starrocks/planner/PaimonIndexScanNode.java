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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DlfUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.List;

public class PaimonIndexScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonIndexScanNode.class);

    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final IndexTable table;
    private final String indexCondition;

    public PaimonIndexScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, IndexTable table,
                               String indexCondition) {
        super(id, desc, planNodeName);
        this.table = table;
        this.indexCondition = indexCondition;
    }

    public IndexTable getTable() {
        return table;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        // Propagate cloud credentials so BE PaimonGlobalIndexScanner can build a JindoClient
        // for OSS-backed tables. For DLF REST catalogs the credential lives in RESTTokenFileIO
        // and must be refreshed per query; for other catalogs we fall back to the catalog-level
        // CloudConfiguration.
        Table innerTable = table.getInnerTable();
        if (innerTable instanceof PaimonTable) {
            PaimonTable paimonTable = (PaimonTable) innerTable;
            CloudConfiguration cloudConfiguration = DlfUtil.buildPaimonCloudConfiguration(paimonTable);
            HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        }
        msg.hdfs_scan_node = tHdfsScanNode;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table: ").append(table.getName()).append("\n");
        output.append(prefix).append("condition: ").append(indexCondition).append("\n");
        return output.toString();
    }

    public void setupScanRangeLocations() {
        ConnectorMetadata metadata = GlobalStateMgr.getCurrentState().getConnectorMgr()
                .getConnector(table.getInnerTable().getCatalogName()).getMetadata();
        try (Timer ignored = Tracers.watchScope(Tracers.Module.OPTIMIZER, "GetIndexShards")) {
            List<Range> shards = new IndexAnalyzer(table.getInnerTable(), metadata)
                    .getIndexShards(it -> (Range) it);
            for (int i = 0; i < shards.size(); i++) {
                Range range = shards.get(i);
                PaimonTable innerTable = (PaimonTable) table.getInnerTable();
                FileStoreTable nativeTable = (FileStoreTable) innerTable.getNativeTable();
                TScanRangeLocations scanRangeLocation = createScanRangeLocation(
                        i, range.from, range.to, indexCondition, (nativeTable).location().toString());
                scanRangeLocationsList.add(scanRangeLocation);
            }
        }
    }

    public static TScanRangeLocations createScanRangeLocation(
            int shardId,
            long rangeFrom,
            long rangeTo,
            String indexCondition,
            String tablePath
    ) {
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setPaimon_global_index_shard_id(shardId);
        hdfsScanRange.setPaimon_global_index_range_from(rangeFrom);
        hdfsScanRange.setPaimon_global_index_range_to(rangeTo);
        hdfsScanRange.setPaimon_global_index_condition(indexCondition);
        hdfsScanRange.setPaimon_table_path(tablePath);
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.setScan_range(scanRange);
        return scanRangeLocations;
    }

}
