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

import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.connector.ConnectorScanRangeSource;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsConnectorScanRangeSource extends ConnectorScanRangeSource {
    private static final Logger LOG = LogManager.getLogger(OdpsConnectorScanRangeSource.class);
    
    private final OdpsTable odpsTable;
    private final TupleDescriptor desc;
    private final RemoteFileInfoSource remoteFileInfoSource;
    
    public OdpsConnectorScanRangeSource(OdpsTable odpsTable,
                                        RemoteFileInfoSource remoteFileInfoSource,
                                        TupleDescriptor desc) {
        this.odpsTable = odpsTable;
        this.remoteFileInfoSource = remoteFileInfoSource;
        this.desc = desc;
    }

    @Override
    public boolean sourceHasMoreOutput() {
        return remoteFileInfoSource.hasMoreOutput();
    }

    @Override
    public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
        List<TScanRangeLocations> res = new ArrayList<>();
        while (hasMoreOutput() && res.size() < maxSize) {
            RemoteFileInfo remoteFileInfo = remoteFileInfoSource.getOutput();
            List<TScanRangeLocations> scanRanges = convertToScanRanges(remoteFileInfo);
            res.addAll(scanRanges);
        }
        return res;
    }

    private List<TScanRangeLocations> convertToScanRanges(RemoteFileInfo remoteFileInfo) {
        OdpsRemoteFileDesc remoteFileDesc = (OdpsRemoteFileDesc) remoteFileInfo.getFiles().get(0);
        OdpsSplitsInfo splitsInfo = remoteFileDesc.getOdpsSplitsInfo();
        
        if (splitsInfo.isEmpty()) {
            LOG.warn("There is no odps splits on {}.{}", 
                    odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName());
            return new ArrayList<>();
        }

        Map<String, String> commonSplitInfo = new HashMap<>();
        String serializeSession = splitsInfo.getSerializeSession();
        commonSplitInfo.put("read_session", serializeSession);
        commonSplitInfo.putAll(splitsInfo.getProperties());
        commonSplitInfo.put("split_policy", splitsInfo.getSplitPolicy().name().toLowerCase());

        List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
        for (InputSplit inputSplit : splitsInfo.getSplits()) {
            TScanRangeLocations scanRangeLocations = createScanRange(inputSplit, commonSplitInfo, splitsInfo);
            scanRangeLocationsList.add(scanRangeLocations);
        }
        return scanRangeLocationsList;
    }

    private TScanRangeLocations createScanRange(InputSplit inputSplit, 
                                               Map<String, String> commonSplitInfo,
                                               OdpsSplitsInfo splitsInfo) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        com.starrocks.thrift.THdfsScanRange hdfsScanRange = new com.starrocks.thrift.THdfsScanRange();
        
        Map<String, String> splitInfo = new HashMap<>(commonSplitInfo);
        splitInfo.put("session_id", inputSplit.getSessionId());
        
        switch (splitsInfo.getSplitPolicy()) {
            case SIZE:
                IndexedInputSplit split = (IndexedInputSplit) inputSplit;
                splitInfo.put("split_index", String.valueOf(split.getSplitIndex()));
                hdfsScanRange.setOffset(split.getSplitIndex());
                break;
            case ROW_OFFSET:
                RowRangeInputSplit split1 = (RowRangeInputSplit) inputSplit;
                splitInfo.put("start_index", String.valueOf(split1.getRowRange().getStartIndex()));
                splitInfo.put("num_record", String.valueOf(split1.getRowRange().getNumRecord()));
                hdfsScanRange.setOffset(split1.getRowRange().getStartIndex());
                break;
            default:
                throw new StarRocksConnectorException(
                        "unsupported split policy: " + splitsInfo.getSplitPolicy().name());
        }
        
        hdfsScanRange.setOdps_split_infos(splitInfo);
        hdfsScanRange.setUse_odps_jni_reader(true);
        hdfsScanRange.setFile_length(1);
        hdfsScanRange.setLength(1);
        
        com.starrocks.thrift.TScanRange scanRange = new com.starrocks.thrift.TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);
        
        com.starrocks.thrift.TScanRangeLocation scanRangeLocation = 
                new com.starrocks.thrift.TScanRangeLocation(
                        new com.starrocks.thrift.TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);
        
        return scanRangeLocations;
    }
}