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

package com.starrocks.scheduler;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TaskBuilderTest {

    @Test
    public void testTaskBuilderForMv() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("aa.bb.cc");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        Task task = TaskBuilder.buildMvTask(mv, "test");
        Assertions.assertEquals("insert overwrite `aa.bb.cc` select * from table1", task.getDefinition());
    }

    @Test
    public void testTaskBuilderForMvWithCreator() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("test_mv");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        
        // Set MV creator
        UserIdentity creator = UserIdentity.createAnalyzedUserIdentWithIp("admin", "%");
        mv.setMvCreator(creator);
        
        // Build task and verify creator is used while ConnectContext.get() is null
        new Expectations() {
            {
                ConnectContext.get();
                result = null;
            }
        };
        Task task = TaskBuilder.buildMvTask(mv, "test_db");
        Assertions.assertNotNull(task.getUserIdentity());
        Assertions.assertEquals("admin", task.getCreateUser());
        Assertions.assertEquals(creator, task.getUserIdentity());
    }

    @Test
    public void testTaskBuilderForMvWithoutCreator() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("test_mv");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        
        // Don't set MV creator, should fall back to default (root)
        Task task = TaskBuilder.buildMvTask(mv, "test_db");
        // When no ConnectContext and no mvCreator, should use default root
        Assertions.assertEquals("root", task.getCreateUser());
    }
}
