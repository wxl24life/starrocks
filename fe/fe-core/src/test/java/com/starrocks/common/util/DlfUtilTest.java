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

package com.starrocks.common.util;

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class DlfUtilTest {
    private static final String DLF_ACCESS_TRACKING_EXTENDED_INFO = "dlf.access-tracking.extended-info";
    private static final String OSS_USER_AGENT_EXTENDED = "fs.oss.user.agent.extended";

    @Test
    public void testGetUserAgentExtendedSetTrackingInfoToUserAgent() {
        Options options = new Options();
        RESTToken token = mockToken("tracking-info-123");

        String actual = DlfUtil.getUserAgentExtended(options.toMap(), token);

        Assert.assertEquals("tracking-info-123", actual);
    }

    @Test
    public void testGetUserAgentExtendedAppendTrackingInfoToExistingUserAgent() {
        Options options = new Options();
        options.set(OSS_USER_AGENT_EXTENDED, "existing-agent");
        RESTToken token = mockToken("tracking-info-456");

        String actual = DlfUtil.getUserAgentExtended(options.toMap(), token);

        Assert.assertEquals("existing-agent tracking-info-456", actual);
    }

    @Test
    public void testGetUserAgentExtendedNoTrackingInfo() {
        Options options = new Options();
        options.set(OSS_USER_AGENT_EXTENDED, "existing-agent");
        RESTToken token = mockToken(null);

        String actual = DlfUtil.getUserAgentExtended(options.toMap(), token);

        Assert.assertEquals("existing-agent", actual);
    }

    @Test
    public void testGetUserAgentExtendedIgnoreBlankTrackingInfo() {
        Options options = new Options();
        options.set(OSS_USER_AGENT_EXTENDED, "existing-agent");
        RESTToken token = mockToken(" ");

        String actual = DlfUtil.getUserAgentExtended(options.toMap(), token);

        Assert.assertEquals("existing-agent", actual);
    }

    private RESTToken mockToken(String trackingInfo) {
        RESTToken token = Mockito.mock(RESTToken.class);
        Map<String, String> tokenMap = new HashMap<>();
        if (trackingInfo != null) {
            tokenMap.put(DLF_ACCESS_TRACKING_EXTENDED_INFO, trackingInfo);
        }
        Mockito.when(token.token()).thenReturn(tokenMap);
        return token;
    }
}
