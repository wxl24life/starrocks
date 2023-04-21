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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.common.util.Util.validateMetastoreUris;

/**
 * Hive resource for external hive table
 * <p>
 * Hive resource example:
 * CREATE EXTERNAL RESOURCE "hive0"
 * PROPERTIES
 * (
 * "type" = "hive",
 * "hive.metastore.uris" = "thrift://hostname:9083"
 * );
 * <p>
 * DROP RESOURCE "hive0";
 */
public class HiveResource extends Resource {

    private static final String MAX_COMPUTE_ENABLE = "maxcompute.enable";
    // require only one property currently
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    @SerializedName(value = "isMaxComputeResource")
    private boolean isMaxComputeResource = false;

    @SerializedName(value = "resourceOptions")
    private final Map<String, String> resourceOptions = new HashMap<>();

    @SerializedName(value = "metastoreURIs")
    private String metastoreURIs = "null";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HiveResource(String name) {
        super(name, ResourceType.HIVE);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null, "properties can not be null");
        this.properties = Maps.newHashMap(properties);
        metastoreURIs = properties.get(HIVE_METASTORE_URIS);
        if (!FeConstants.runningUnitTest) {
            isMaxComputeResource = Boolean.parseBoolean(properties.getOrDefault(MAX_COMPUTE_ENABLE, "false"));
            if (!isMaxComputeResource) {
                metastoreURIs = properties.get(HIVE_METASTORE_URIS);
                if (StringUtils.isBlank(metastoreURIs)) {
                    throw new DdlException(HIVE_METASTORE_URIS + " must be set in properties");
                }
                validateMetastoreUris(metastoreURIs);
            } else {
                properties.remove(HIVE_METASTORE_URIS);
                resourceOptions.putAll(properties);
            }
        }
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        if (!isMaxComputeResource) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, HIVE_METASTORE_URIS, metastoreURIs));
        } else {
            result.addRow(Lists.newArrayList(name, lowerCaseType, MAX_COMPUTE_ENABLE, "true"));
            for (Map.Entry<String, String> entry : resourceOptions.entrySet()) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }

    public String getHiveMetastoreURIs() {
        return metastoreURIs;
    }

    public Map<String, String> getProperties() {
        return properties == null ? Maps.newHashMap() : properties;
    }

    public boolean isMaxComputeResource() {
        return isMaxComputeResource;
    }

    public Map<String, String> getResourceOptions() {
        return resourceOptions;
    }

    /**
     * <p>alter the resource properties.</p>
     * <p>the user can not alter the property that the system does not support.
     * currently , hive resource only support 'hive.metastore.uris' property to alter. </p>
     *
     * @param properties the properties that user uses to alter
     * @throws DdlException
     */
    public void alterProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null, "properties can not be null");

        if (isMaxComputeResource) {
            if (properties.containsKey(HIVE_METASTORE_URIS)) {
                throw new DdlException("Couldn't add " + HIVE_METASTORE_URIS +
                    " when this resource is actually a MaxCompute resource.");
            }
            resourceOptions.putAll(properties);
        } else {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (HIVE_METASTORE_URIS.equals(key)) {
                    if (StringUtils.isBlank(value)) {
                        throw new DdlException(HIVE_METASTORE_URIS + " can not be null");
                    }
                    validateMetastoreUris(value);
                    this.metastoreURIs = value;
                } else {
                    throw new DdlException(String.format("property %s has not support yet", key));
                }
            }
        }
    }

    public String getDdlStmt() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL RESOURCE \"");
        sb.append(name);
        sb.append("\" PROPERTIES (");
        sb.append("\"type\" = \"");
        sb.append(type);
        sb.append("\", \"hive.metastore.uris\" = \"");
        sb.append(metastoreURIs);
        sb.append("\");");
        return sb.toString();
    }
}
