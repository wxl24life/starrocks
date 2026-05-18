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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class IcebergViewTest {

    private static IcebergView newView() {
        return new IcebergView(1, "uup_catalog", "ads_cockpit_qck", "dim_store_test",
                Collections.emptyList(), "select * from dim_dm.dim_store", "location");
    }

    @Test
    public void testFormatRelationsOverridesPreNormalizedCatalog() {
        // Reproduces the production bug: by the time formatRelations runs, an earlier
        // normalization step has already filled TableName.catalog with the session's
        // current catalog (e.g. default_catalog). Without overriding, view internal
        // references would resolve against the wrong catalog and fail.
        IcebergView view = newView();
        TableRelation relation = new TableRelation(new TableName("default_catalog", "dim_dm", "dim_store"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        Assertions.assertEquals("uup_catalog", relation.getName().getCatalog());
        Assertions.assertEquals("dim_dm", relation.getName().getDb());
    }

    @Test
    public void testFormatRelationsFillsViewDbWhenRelationDbIsEmpty() {
        IcebergView view = newView();
        TableRelation relation = new TableRelation(new TableName(null, null, "dim_store"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        Assertions.assertEquals("uup_catalog", relation.getName().getCatalog());
        Assertions.assertEquals("ads_cockpit_qck", relation.getName().getDb());
    }

    @Test
    public void testFormatRelationsOverridesEvenWhenRelationCatalogIsNull() {
        IcebergView view = newView();
        TableRelation relation = new TableRelation(new TableName(null, "dim_dm", "dim_store"));

        view.formatRelations(Lists.newArrayList(relation), Collections.emptyList());

        Assertions.assertEquals("uup_catalog", relation.getName().getCatalog());
        Assertions.assertEquals("dim_dm", relation.getName().getDb());
    }

}
