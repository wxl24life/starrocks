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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * EMR Serverless Spark job state enum.
 * Maps EMR state strings to TEtlState and SparkLoadAppHandle.State.
 */
public enum ServerlessSparkState {
    SUBMITTED("submitted", TEtlState.RUNNING, SparkLoadAppHandle.State.SUBMITTED),
    RUNNING("running", TEtlState.RUNNING, SparkLoadAppHandle.State.RUNNING),
    CANCELLING("cancelling", TEtlState.RUNNING, SparkLoadAppHandle.State.RUNNING),
    SUCCESS("success", TEtlState.FINISHED, SparkLoadAppHandle.State.FINISHED),
    FAILED("failed", TEtlState.CANCELLED, SparkLoadAppHandle.State.FAILED),
    CANCELLED("cancelled", TEtlState.CANCELLED, SparkLoadAppHandle.State.KILLED);
    private static final Pattern STATE_IS_PATTERN = Pattern.compile("state is:\\s*(\\w+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN = Pattern.compile("\\b(submitted|running|success|failed|cancelled|cancelling)\\b",
            Pattern.CASE_INSENSITIVE);

    private final String value;
    private final TEtlState etlState;
    private final SparkLoadAppHandle.State sparkState;

    ServerlessSparkState(String value, TEtlState etlState, SparkLoadAppHandle.State sparkState) {
        this.value = value;
        this.etlState = etlState;
        this.sparkState = sparkState;
    }

    public String getValue() {
        return value;
    }

    public TEtlState getEtlState() {
        return etlState;
    }

    public SparkLoadAppHandle.State getSparkState() {
        return sparkState;
    }

    /**
     * Parse ServerlessSparkState from string value (case insensitive).
     */
    public static ServerlessSparkState fromValue(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (ServerlessSparkState state : values()) {
            if (state.value.equalsIgnoreCase(value)) {
                return state;
            }
        }
        return null;
    }

    /**
     * Parse ServerlessSparkState from spark-submit --status output.
     *
     * @throws LoadException if no valid state is found
     */
    public static ServerlessSparkState fromOutput(String output) throws LoadException {
        // Try to find state in the last non-empty line
        String[] lines = output.split("\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            Matcher m = PATTERN.matcher(line);
            if (m.find()) {
                ServerlessSparkState state = fromValue(m.group(1));
                if (state != null) {
                    return state;
                }
            }
        }
        throw new LoadException("Failed to parse EMR state from output: " + output);
    }

    /**
     * Parse Serverless Spark state from spark-submit --status output.
     *
     * @return the parsed ServerlessSparkState, or null if no valid state is found
     */
    public static ServerlessSparkState fromLauncherLog(String output) {
        // Try to match "state is: xxx" pattern
        Matcher stateMatcher = STATE_IS_PATTERN.matcher(output);
        if (stateMatcher.find()) {
            ServerlessSparkState state = fromValue(stateMatcher.group(1));
            if (state != null) {
                return state;
            }
        }
        return null;
    }
}
