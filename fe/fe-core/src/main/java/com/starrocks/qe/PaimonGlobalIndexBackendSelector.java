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

package com.starrocks.qe;

import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.planner.PaimonIndexScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.INDEX;
import static com.starrocks.connector.index.IndexTable.INDEX_TABLE_SUFFIX;

public class PaimonGlobalIndexBackendSelector implements BackendSelector {

    private final PaimonIndexScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;
    private final SessionVariable sessionVariable;
    private final Map<ComputeNode, Assignment> node2assignment = Maps.newHashMap();

    public static class Assignment {
        public int count;
        public int searchListSize;
        public int n;
        public final List<String> shards = new ArrayList<>();
    }

    public PaimonGlobalIndexBackendSelector(
            PaimonIndexScanNode paimonIndexScanNode,
            List<TScanRangeLocations> locations,
            FragmentScanRangeAssignment assignment,
            WorkerProvider workerProvider,
            SessionVariable sessionVariable) {
        this.locations = locations;
        this.scanNode = paimonIndexScanNode;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.sessionVariable = sessionVariable;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        // workerProvider is already constructed for this query's warehouse; no extra warehouse
        // filter needed (and a hard-coded DEFAULT filter would wrongly drop non-default workers).
        List<ComputeNode> availableWorkers = workerProvider.getAllWorkers().stream()
                .sorted(Comparator.comparingLong(ComputeNode::getId))
                .collect(Collectors.toList());

        if (availableWorkers.isEmpty()) {
            throw new StarRocksException(
                    "No alive backend in warehouse " + workerProvider.getWarehouseId()
                            + " for paimon global index scan");
        }

        String tableName = scanNode.getTable().getName().replace(INDEX_TABLE_SUFFIX, "");
        computeScanRangeAssignmentSimple(
                availableWorkers,
                node2assignment,
                locations,
                (backendId, scanRangeParams) -> assignment.put(backendId, scanNode.getId().asInt(), scanRangeParams)
        );

        List<String> nodeAssignmentList = new ArrayList<>();
        for (ComputeNode computeNode : node2assignment.keySet()) {
            Assignment nodeAssignment = node2assignment.get(computeNode);
            nodeAssignmentList.add(String.format("\"%d, %d, %d, %d, %s\"",
                    computeNode.getId(), nodeAssignment.count, nodeAssignment.n, nodeAssignment.searchListSize,
                    String.format("[%s]", String.join(", ", nodeAssignment.shards))));
        }

        String profile = String.format("[\"nodeId, shardCount, totalN, totalSearchListSize, shards\", %s]",
                String.join(",", nodeAssignmentList));
        String prefix = "Paimon.GlobalIndex.";
        Tracers.record(INDEX, prefix + tableName + ".evaluateGlobalIndex.backendAssignment", profile);
    }

    public static void computeScanRangeAssignmentSimple(
            List<ComputeNode> availableWorkers,
            Map<ComputeNode, Assignment> node2assignment,
            List<TScanRangeLocations> locations,
            BiConsumer<Long, TScanRangeParams> assignmentPut
    ) {
        // Initialize assignments for all available workers
        availableWorkers.forEach(computeNode -> node2assignment.put(computeNode, new Assignment()));

        // Build mapping from shardId to location, sorted by shardId
        Map<Integer, TScanRangeLocations> shardIdToLocation = new HashMap<>();
        for (TScanRangeLocations location : locations) {
            int shardId = getIndexShardId(location);
            shardIdToLocation.put(shardId, location);
        }

        // Sort shard IDs in ascending order
        List<Integer> sortedShardIds = shardIdToLocation.keySet().stream().sorted().collect(Collectors.toList());

        // Sequential allocation: assign each shard to the next available BE (round-robin across available BEs)
        int beIndex = 0;
        AtomicReference<int[]> localNRef = new AtomicReference<>();
        for (Integer shardId : sortedShardIds) {
            TScanRangeLocations location = shardIdToLocation.get(shardId);
            // Predicate-only condition has no per-shard "n"; treat localN as 0 so the
            // search-list-size accounting falls back to its default 1024.
            int localN = getIndexShardLocalN(location, localNRef);
            int searchListSize = localN > 1024 ? localN * 3 / 2 : 1024;

            // Get the next available BE (cycle through available BEs)
            ComputeNode selectedWorker = availableWorkers.get(beIndex % availableWorkers.size());
            Assignment nodeAssignment = node2assignment.get(selectedWorker);
            nodeAssignment.count++;
            nodeAssignment.searchListSize += searchListSize;
            nodeAssignment.n += localN;
            nodeAssignment.shards.add(String.valueOf(shardId));

            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = location.scan_range;
            assignmentPut.accept(selectedWorker.getId(), scanRangeParams);

            beIndex++;
        }
    }

    public static int getIndexShardLocalN(TScanRangeLocations location, AtomicReference<int[]> localNRef) {
        THdfsScanRange hdfsScanRange = location.getScan_range().getHdfs_scan_range();
        int shardId = (int) hdfsScanRange.getPaimon_global_index_shard_id();
        if (localNRef.get() == null) {
            JSONObject indexCondition = new JSONObject(hdfsScanRange.getPaimon_global_index_condition());
            // Predicate-only IndexCondition does not carry "n" (only TopNIndexCondition does).
            // Without this branch the BackendSelector would throw JSONException for every
            // bitmap/bsi-index query.
            if (!indexCondition.has("n")) {
                return 0;
            }
            JSONArray nJsonArray = indexCondition.getJSONArray("n");
            int[] localN = new int[nJsonArray.length()];
            for (int i = 0; i < nJsonArray.length(); i++) {
                localN[i] = nJsonArray.getInt(i);
            }
            localNRef.set(localN);
        }
        int[] localN = localNRef.get();
        if (shardId < 0 || shardId >= localN.length) {
            // Defensive: shardId comes from FE-supplied scan range; mismatch with the "n" array
            // length signals inconsistent metadata between the planner and the BackendSelector.
            throw new IllegalStateException(String.format(
                    "shardId %d out of range for paimon global index localN array (length=%d)",
                    shardId, localN.length));
        }
        return localN[shardId];
    }

    public static int getIndexShardId(TScanRangeLocations location) {
        THdfsScanRange hdfsScanRange = location.getScan_range().getHdfs_scan_range();
        return (int) hdfsScanRange.getPaimon_global_index_shard_id();
    }

}
