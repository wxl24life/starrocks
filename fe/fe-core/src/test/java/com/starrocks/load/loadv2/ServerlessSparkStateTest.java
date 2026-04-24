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

package com.starrocks.load.loadv2;

import com.starrocks.common.LoadException;
import com.starrocks.thrift.TEtlState;
import org.junit.Assert;
import org.junit.Test;

public class ServerlessSparkStateTest {

    @Test
    public void testFromValue() {
        Assert.assertEquals(ServerlessSparkState.SUBMITTED, ServerlessSparkState.fromValue("submitted"));
        Assert.assertEquals(ServerlessSparkState.RUNNING, ServerlessSparkState.fromValue("Running"));
        Assert.assertEquals(ServerlessSparkState.SUCCESS, ServerlessSparkState.fromValue("SUCCESS"));
        Assert.assertEquals(ServerlessSparkState.FAILED, ServerlessSparkState.fromValue("failed"));
        Assert.assertEquals(ServerlessSparkState.CANCELLED, ServerlessSparkState.fromValue("cancelled"));
        Assert.assertEquals(ServerlessSparkState.CANCELLING, ServerlessSparkState.fromValue("cancelling"));
        Assert.assertNull(ServerlessSparkState.fromValue(null));
        Assert.assertNull(ServerlessSparkState.fromValue(""));
        Assert.assertNull(ServerlessSparkState.fromValue("unknown"));
    }

    @Test
    public void testFromOutput() throws LoadException {
        // normal case
        ServerlessSparkState state = ServerlessSparkState.fromOutput("job state: success\n");
        Assert.assertEquals(ServerlessSparkState.SUCCESS, state);
        Assert.assertEquals(TEtlState.FINISHED, state.getEtlState());

        state = ServerlessSparkState.fromOutput("status: running\n");
        Assert.assertEquals(ServerlessSparkState.RUNNING, state);
        Assert.assertEquals(TEtlState.RUNNING, state.getEtlState());

        state = ServerlessSparkState.fromOutput("application failed\n");
        Assert.assertEquals(ServerlessSparkState.FAILED, state);
        Assert.assertEquals(TEtlState.CANCELLED, state.getEtlState());
    }

    @Test
    public void testFromOutputSkipsSLF4JLines() throws LoadException {
        String output = "SLF4J: some warning\nstatus: submitted\n";
        ServerlessSparkState state = ServerlessSparkState.fromOutput(output);
        Assert.assertEquals(ServerlessSparkState.SUBMITTED, state);
    }

    @Test(expected = LoadException.class)
    public void testFromOutputNoState() throws LoadException {
        ServerlessSparkState.fromOutput("no valid state here\n");
    }

    @Test
    public void testWordBoundary() throws LoadException {
        // "successfully" should NOT match "success"
        Assert.assertThrows(LoadException.class,
                () -> ServerlessSparkState.fromOutput("job completed successfully\n"));

        // "unsubmitted" should NOT match "submitted"
        Assert.assertThrows(LoadException.class,
                () -> ServerlessSparkState.fromOutput("task is unsubmitted\n"));

        // exact word "success" should still match
        ServerlessSparkState state = ServerlessSparkState.fromOutput("final state: success\n");
        Assert.assertEquals(ServerlessSparkState.SUCCESS, state);
    }

    @Test
    public void testFromLauncherLog() {
        ServerlessSparkState state = ServerlessSparkState.fromLauncherLog("state is: running");
        Assert.assertEquals(ServerlessSparkState.RUNNING, state);

        state = ServerlessSparkState.fromLauncherLog("state is: success");
        Assert.assertEquals(ServerlessSparkState.SUCCESS, state);

        state = ServerlessSparkState.fromLauncherLog("no state info here");
        Assert.assertNull(state);
    }

    @Test
    public void testGetSparkState() {
        Assert.assertEquals(SparkLoadAppHandle.State.SUBMITTED, ServerlessSparkState.SUBMITTED.getSparkState());
        Assert.assertEquals(SparkLoadAppHandle.State.RUNNING, ServerlessSparkState.RUNNING.getSparkState());
        Assert.assertEquals(SparkLoadAppHandle.State.FINISHED, ServerlessSparkState.SUCCESS.getSparkState());
        Assert.assertEquals(SparkLoadAppHandle.State.FAILED, ServerlessSparkState.FAILED.getSparkState());
        Assert.assertEquals(SparkLoadAppHandle.State.KILLED, ServerlessSparkState.CANCELLED.getSparkState());
    }
}
