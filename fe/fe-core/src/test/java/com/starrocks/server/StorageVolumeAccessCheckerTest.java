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

package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.HdfsUtil;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageVolumeAccessCheckerTest {

    private Map<String, String> buildParams() {
        Map<String, String> params = new HashMap<>();
        params.put(CloudConfigurationConstants.AWS_S3_REGION, "us-east-1");
        params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "https://s3.amazonaws.com");
        params.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak");
        params.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "sk");
        return params;
    }

    @Test
    public void testCheckPassesWhenPathExists() throws DdlException {
        new MockUp<HdfsUtil>() {
            @Mock
            public boolean checkPathExist(String remotePath, Map<String, String> properties) throws StarRocksException {
                return true;
            }
        };

        List<String> locations = Arrays.asList("s3://bucket/path");
        // Should not throw
        StorageVolumeAccessChecker.check("test_sv", "S3", locations, buildParams());
    }

    @Test
    public void testCheckFailsWithDdlExceptionOnAccessError() {
        new MockUp<HdfsUtil>() {
            @Mock
            public boolean checkPathExist(String remotePath, Map<String, String> properties) throws StarRocksException {
                throw new StarRocksException("Forbidden (Status Code: 403)");
            }
        };

        List<String> locations = Arrays.asList("s3://bucket/path");
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check("test_sv", "S3", locations, buildParams()));
        Assertions.assertTrue(ex.getMessage().contains("accessibility check failed"));
        Assertions.assertTrue(ex.getMessage().contains("test_sv"));
        Assertions.assertTrue(ex.getMessage().contains("s3://bucket/path"));
        Assertions.assertTrue(ex.getMessage().contains("403"));
    }

    @Test
    public void testCheckFailsOnFirstFailingLocation() {
        List<String> callOrder = new java.util.ArrayList<>();
        new MockUp<HdfsUtil>() {
            @Mock
            public boolean checkPathExist(String remotePath, Map<String, String> properties) throws StarRocksException {
                callOrder.add(remotePath);
                if (remotePath.contains("bad")) {
                    throw new StarRocksException("Access denied");
                }
                return true;
            }
        };

        List<String> locations = Arrays.asList("s3://bucket/good", "s3://bucket/bad", "s3://bucket/other");
        Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check("test_sv", "S3", locations, buildParams()));
        // Should stop at the bad location, not proceed to "other"
        Assertions.assertEquals(2, callOrder.size());
        Assertions.assertEquals("s3://bucket/bad", callOrder.get(1));
    }

    @Test
    public void testCheckIncludesRootCauseMessage() {
        new MockUp<HdfsUtil>() {
            @Mock
            public boolean checkPathExist(String remotePath, Map<String, String> properties) throws StarRocksException {
                RuntimeException root = new RuntimeException("root cause detail");
                throw new StarRocksException("wrapped", root);
            }
        };

        List<String> locations = Arrays.asList("s3://bucket/path");
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> StorageVolumeAccessChecker.check("test_sv", "S3", locations, buildParams()));
        Assertions.assertTrue(ex.getMessage().contains("root cause detail"),
                "Error message should include root cause: " + ex.getMessage());
    }
}
