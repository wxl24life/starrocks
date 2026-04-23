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

package com.starrocks.common.util;

import com.aliyun.datalake.common.DlfDataToken;
import com.aliyun.datalake.common.impl.Base64Util;
import com.aliyun.datalake.external.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.aliyun.datalake.core.constant.DataLakeConfig.FS_OSS_ENDPOINT;
import static com.starrocks.StarRocksFE.STARROCKS_HOME_DIR;

public class DlfUtil {
    private static final Logger LOG = LogManager.getLogger(DlfUtil.class);
    private static final String DLF_ACCESS_TRACKING_EXTENDED_INFO = "dlf.access-tracking.extended-info";
    private static final String OSS_USER_AGENT_KEY = "fs.oss.user.agent.extended";
    private static Configuration conf = null;

    public static ConnectContext getQueryContext() {
        ConnectContext.setContextUserIfNeeded();
        return ConnectContext.get();
    }

    public static String getRamUser() {
        if (ConnectContext.get() != null) {
            UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            if (userIdentity != null) {
                return getRamUser(userIdentity.getUser());
            } else if (!Strings.isNullOrEmpty(qualifiedUser)) {
                return getRamUser(qualifiedUser);
            }
        }
        // Some background threads may not have created a ConnectContext or set a user
        // In such cases, we use ROOT_USER, and add logging for observability.
        LOG.warn("User is not set when accessing dlf, use ROOT_USER, stack: {}", LogUtil.getCurrentStackTrace());
        return getRamUser(AuthenticationMgr.ROOT_USER);
    }

    public static String getRamUser(String user) {
        if (!Strings.isNullOrEmpty(user)) {
            return GlobalStateMgr.getCurrentState().getAuthenticationMgr().getRamUser(user);
        }
        return "";
    }

    public static Configuration readHadoopConf() {
        if (null != conf) {
            return conf;
        }
        Configuration conf = new Configuration();
        String confPath = STARROCKS_HOME_DIR + "/conf/core-site.xml";
        try {
            Path path = Paths.get(confPath);
            if (Files.exists(path)) {
                String xml = new String(Files.readAllBytes(path));
                HadoopUtils.readHadoopXml(xml, conf);
            } else {
                LOG.warn("Cannot find core-site.xml.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DlfUtil.conf = conf;
        return DlfUtil.conf;
    }

    public static String getMetaToken(String ramUser) {
        // fixed currently, maybe can get from config later
        return "/secret/DLF/meta/" + Base64Util.encodeBase64WithoutPadding(ramUser);
    }

    public static Map<String, String> setDataToken(File dataTokenFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DlfDataToken dataToken = mapper.readValue(dataTokenFile, DlfDataToken.class);
        Map<String, String> properties = new HashMap<>();
        Configuration conf = DlfUtil.readHadoopConf();
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, dataToken.getAccessKeyId());
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, dataToken.getAccessKeySecret());
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, conf.get(FS_OSS_ENDPOINT));
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN, dataToken.getSecurityToken());
        return properties;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getFieldValue(Object obj, String fieldName) {
        Field field;
        try {
            field = obj.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        field.setAccessible(true);
        try {
            return (T) field.get(obj);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getUserAgentExtended(Map<String, String> options, RESTToken token) {
        String baseAgent = options.get(OSS_USER_AGENT_KEY);
        String trackingInfo = token.token().get(DLF_ACCESS_TRACKING_EXTENDED_INFO);
        if (Strings.isNullOrEmpty(trackingInfo) || trackingInfo.trim().isEmpty()) {
            return baseAgent;
        }
        if (Strings.isNullOrEmpty(baseAgent) || baseAgent.trim().isEmpty()) {
            return trackingInfo;
        }
        return baseAgent + " " + trackingInfo;
    }
}
