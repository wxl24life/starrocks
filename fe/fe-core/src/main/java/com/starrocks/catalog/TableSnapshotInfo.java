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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Table.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

/**
 * Snapshot read for time travel
 * supports external table
 */
public class TableSnapshotInfo {

    public enum VersionType {
        TIMESTAMP_MILLIS, TIMESTAMP, VERSION
    }

    private final VersionType type;
    private String value;

    public TableSnapshotInfo(TableSnapshotInfo other) {
        this.type = other.type;
        this.value = other.value;
    }

    public TableSnapshotInfo(String value, VersionType type) {
        this.value = value;
        this.type = type;
    }

    public static TableSnapshotInfo timestampMillisOf(String timestampMillis) {
        return new TableSnapshotInfo(timestampMillis, VersionType.TIMESTAMP_MILLIS);
    }

    public static TableSnapshotInfo timestampOf(String timestamp) {
        return new TableSnapshotInfo(timestamp, VersionType.TIMESTAMP);
    }

    public static TableSnapshotInfo versionOf(String version) {
        return new TableSnapshotInfo(version, VersionType.VERSION);
    }

    public String getValue() {
        return value;
    }

    public VersionType getType() {
        return type;
    }

    public void setValue(String value) {
        this.value = value;
    }

}