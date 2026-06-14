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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
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
    private static final String DLF_ACCESS_TRACKING_EXTENDED_INFO =
            "dlf.access-tracking.extended-info";
    private static Configuration conf = null;

    public static String getRamUser() {
        String user = getCurrentUser();
        if (!Strings.isNullOrEmpty(user)) {
            return GlobalStateMgr.getCurrentState().getAuthenticationMgr().getRamUser(user);
        }
        return "";
    }

    private static String getCurrentUser() {
        if (ConnectContext.get() != null) {
            UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            if (userIdentity != null) {
                return userIdentity.getUser();
            } else if (!Strings.isNullOrEmpty(qualifiedUser)) {
                return qualifiedUser;
            }
        }
        // Background threads (stats-cache-refresher, checkpoint, txn-timeout-checker, ...) routinely
        // access DLF without a ConnectContext; the ROOT_USER fallback is by design, not an anomaly.
        // Log at DEBUG without a stack trace: full-stack WARN here filled fe.warn.log at ~5MB/s under
        // stats-refresher fanout, blowing /mnt/disk1 to 100% and crashing FE.
        LOG.debug("User is not set when accessing dlf, use ROOT_USER");
        return AuthenticationMgr.ROOT_USER;
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

    public static String buildUserAgentExtended(String catalogName) {
        String userExtended = catalogName + "_" + getCurrentUser();
        if (Util.isRootUser()) {
            return "starrocks/internal";
        } else {
            return String.format("starrocks/%s", userExtended);
        }
    }

    public static CloudConfiguration buildPaimonCloudConfiguration(PaimonTable paimonTable) {
        String userAgentExtended = buildUserAgentExtended(paimonTable.getCatalogName());

        if (!(paimonTable.getNativeTable().fileIO() instanceof RESTTokenFileIO)) {
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr()
                    .getConnector(paimonTable.getCatalogName());
            Preconditions.checkState(connector != null,
                    String.format("connector of catalog %s should not be null", paimonTable.getCatalogName()));
            CloudConfiguration cloudConfiguration = connector.getMetadata().getCloudConfiguration();
            Preconditions.checkState(cloudConfiguration != null,
                    String.format("cloudConfiguration of catalog %s should not be null", paimonTable.getCatalogName()));
            if (cloudConfiguration instanceof AliyunCloudConfiguration) {
                AliyunCloudCredential oldCred = ((AliyunCloudConfiguration) cloudConfiguration).getAliyunCloudCredential();
                Map<String, String> properties = new HashMap<>();
                oldCred.toThrift(properties);
                properties.put(CloudConfigurationConstants.ALIYUN_OSS_USER_AGENT_EXTENDED, userAgentExtended);
                return CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            }
            return cloudConfiguration;
        }
        RESTTokenFileIO fileIO = (RESTTokenFileIO) paimonTable.getNativeTable().fileIO();
        RESTToken token = fileIO.validToken();
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY,
                token.token().get(AliyunCloudCredential.FS_OSS_ACCESS_KEY));
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY,
                token.token().get(AliyunCloudCredential.FS_OSS_SECRET_KEY));
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN,
                token.token().get(AliyunCloudCredential.FS_OSS_SECURITY_TOKEN));
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT,
                token.token().get(AliyunCloudCredential.FS_OSS_ENDPOINT));
        String extendedInfo = token.token().get(DLF_ACCESS_TRACKING_EXTENDED_INFO);
        String ossUserAgent = Strings.isNullOrEmpty(extendedInfo)
                ? userAgentExtended : userAgentExtended + " " + extendedInfo;
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_USER_AGENT_EXTENDED, ossUserAgent);
        return CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
    }
}
