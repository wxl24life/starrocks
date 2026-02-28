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

package com.starrocks.sql.plan;

import com.starrocks.common.DdlException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for Paimon table partition pruning functionality.
 * 
 * Note: The existing partitioned_table in ConnectorPlanTestBase has pt(DATE) column
 * which uses dynamic values (LocalDate.now() + i), making it hard to test.
 * So we need to create our own test tables with fixed partition values.
 */
public class PaimonPartitionPruneTest extends ConnectorPlanTestBase {
    
    @Before
    public void setUp() {
        super.setUp();
        try {
            connectContext.changeCatalogDb("paimon0.pmn_db1");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testPaimonPartitionPrune() throws Exception {
        // partitioned_table has 10 partitions with pt = LocalDate.now() + i (i=0..9)
        // Test exact match - should return 1 partition
        String sql = "select * from partitioned_table where pt = current_date();";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "0:PaimonScanNode");
        assertContains(plan, "TABLE: partitioned_table");
        assertContains(plan, "PARTITION PREDICATES:");
        // Should scan 1 partition out of 10
        assertContains(plan, "partitions=1/10");
    }

    @Test
    public void testPaimonPartitionWithNonPartitionPredicate() throws Exception {
        // Test partition predicate combined with non-partition predicate
        String sql = "select * from partitioned_table where pk = '1';";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:PaimonScanNode");
        assertContains(plan, "NON-PARTITION PREDICATES:");
        assertContains(plan, "partitions=10/10");
    }

    @Test
    public void testPaimonPartitionRange() throws Exception {
        // Test range predicate on partition column
        String sql = "select * from partitioned_table where pt >= current_date() and pt < date_add(current_date(), 3);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:PaimonScanNode");
        assertContains(plan, "PARTITION PREDICATES:");
        // Should scan multiple partitions
        assertContains(plan, "partitions=3/10");
    }

    @Test
    public void testPaimonCompoundPartitionPrune() throws Exception {
        // Test conflicting partition predicates (should result in empty set)
        String sql = "select * from partitioned_table where pt = current_date() and pt = date_add(current_date(), 1);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");
    }

    @Test
    public void testPaimonUnpartitionedTable() throws Exception {
        // For unpartitioned tables, all rows should be scanned
        String sql = "select * from unpartitioned_table;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PaimonScanNode");
        assertContains(plan, "TABLE: unpartitioned_table");
    }
}
