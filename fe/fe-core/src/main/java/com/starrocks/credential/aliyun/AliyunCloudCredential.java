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

package com.starrocks.credential.aliyun;

import com.google.common.base.Preconditions;
import com.staros.proto.AliyunCredentialInfo;
import com.staros.proto.AliyunDefaultCredentialInfo;
import com.staros.proto.AliyunSimpleCredentialInfo;
import com.staros.proto.AliyunStsFileCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.OSSFileStoreInfo;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class AliyunCloudCredential implements CloudCredential {
    private final String accessKey;
    private final String secretKey;
    private final String endpoint;
    private final String region;
    private final String stsFilePath;
    private final boolean useDefaultCredential;

    public AliyunCloudCredential(String accessKey, String secretKey, String endpoint) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(endpoint);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.region = null;
        this.stsFilePath = null;
        this.useDefaultCredential = false;
    }

    public AliyunCloudCredential(String accessKey, String secretKey, String endpoint, String region, String stsFilePath,
                                 boolean useDefaultCredential) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(region);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.region = region;
        this.stsFilePath = stsFilePath;
        this.useDefaultCredential = useDefaultCredential;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        configuration.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        configuration.set("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");
        configuration.set("fs.oss.accessKeyId", accessKey);
        configuration.set("fs.oss.accessKeySecret", secretKey);
        configuration.set("fs.oss.endpoint", endpoint);
    }

    @Override
    public boolean validate() {
        if (!this.stsFilePath.isEmpty() || this.useDefaultCredential) {
            return !this.endpoint.isEmpty();
        }
        return !this.accessKey.isEmpty() && !this.secretKey.isEmpty() && !this.endpoint.isEmpty();
    }

    // reuse aws client logic of BE
    @Override
    public void toThrift(Map<String, String> properties) {
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, accessKey);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, secretKey);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, endpoint);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_STS_FILE_PATH, stsFilePath);
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_USE_DEFAULT_CREDENTIAL,
                String.valueOf(useDefaultCredential));
    }

    @Override
    public String toCredString() {
        return "AliyunCloudCredential{" +
                "accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", stsFilePath=" + stsFilePath + '\'' +
                ", useDefaultCredential=" + useDefaultCredential +
                '}';
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getStsFilePath() {
        return stsFilePath;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.OSS);
        OSSFileStoreInfo.Builder ossFileStoreInfo = OSSFileStoreInfo.newBuilder();
        ossFileStoreInfo.setRegion(region).setEndpoint(endpoint);
        AliyunCredentialInfo.Builder aliyunCredentialInfo = AliyunCredentialInfo.newBuilder();
        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            AliyunSimpleCredentialInfo.Builder simpleCredentialInfo = AliyunSimpleCredentialInfo.newBuilder();
            simpleCredentialInfo.setAccessKey(accessKey);
            simpleCredentialInfo.setAccessKeySecret(secretKey);
            aliyunCredentialInfo.setSimpleCredential(simpleCredentialInfo.build());
        } else if (!stsFilePath.isEmpty()) {
            AliyunStsFileCredentialInfo.Builder stsFileCredentialInfo = AliyunStsFileCredentialInfo.newBuilder();
            stsFileCredentialInfo.setStsFilePath(stsFilePath);
            aliyunCredentialInfo.setStsFileCredential(stsFileCredentialInfo);
        } else if (useDefaultCredential) {
            AliyunDefaultCredentialInfo.Builder defaultCredentialInfo = AliyunDefaultCredentialInfo.newBuilder();
            aliyunCredentialInfo.setDefaultCredential(defaultCredentialInfo);
        } else {
            Preconditions.checkArgument(false, "Unreachable");
        }
        ossFileStoreInfo.setCredential(aliyunCredentialInfo.build());
        fileStore.setOssFsInfo(ossFileStoreInfo.build());
        return fileStore.build();
    }
}
