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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * AI Model resource for external AI service configuration.
 * <p>
 * Example:
 * CREATE EXTERNAL RESOURCE "my_deepseek"
 * PROPERTIES
 * (
 *   "type" = "ai_model",
 *   "endpoint" = "https://api.deepseek.com/compatible-mode/v1/chat/completions",
 *   "model" = "deepseek-chat",
 *   "api_key" = "sk-xxx"
 * );
 * <p>
 * DROP RESOURCE "my_deepseek";
 */
public class AIModelResource extends Resource {

    public static final String ENDPOINT = "endpoint";
    public static final String MODEL = "model";
    public static final String API_KEY = "api_key";
    public static final String PROVIDER = "provider";
    private static final String TYPE_KEY = "type";

    public static final String DEFAULT_PROVIDER = "openai_compatible";
    private static final String API_KEY_MASK = "***";

    private static final Set<String> SUPPORTED_PROVIDERS =
            ImmutableSet.of("openai_compatible");
    private static final Set<String> RESERVED_KEYS =
            ImmutableSet.of(TYPE_KEY, ENDPOINT, MODEL, API_KEY, PROVIDER);

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public AIModelResource(String name) {
        super(name, ResourceType.AI_MODEL);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkArgument(properties != null, "properties can not be null");
        configs = Maps.newHashMap(properties);

        if (StringUtils.isBlank(configs.get(ENDPOINT))) {
            throw new DdlException("'" + ENDPOINT + "' must be set in properties");
        }
        if (StringUtils.isBlank(configs.get(MODEL))) {
            throw new DdlException("'" + MODEL + "' must be set in properties");
        }
        if (StringUtils.isBlank(configs.get(API_KEY))) {
            throw new DdlException("'" + API_KEY + "' must be set in properties");
        }
        configs.putIfAbsent(PROVIDER, DEFAULT_PROVIDER);
        validateProvider(configs.get(PROVIDER));
    }

    public void alterProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkArgument(properties != null, "properties can not be null");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (TYPE_KEY.equals(key)) {
                continue;
            }
            if (ENDPOINT.equals(key) || MODEL.equals(key) || API_KEY.equals(key)) {
                if (StringUtils.isBlank(value)) {
                    throw new DdlException("'" + key + "' can not be empty");
                }
            } else if (PROVIDER.equals(key)) {
                validateProvider(value);
            }
            configs.put(key, value);
        }
    }

    private static void validateProvider(String provider) throws DdlException {
        if (StringUtils.isNotBlank(provider) && !SUPPORTED_PROVIDERS.contains(provider)) {
            throw new DdlException("Unsupported provider '" + provider +
                    "', supported providers: " + SUPPORTED_PROVIDERS);
        }
    }

    @Override
    public String getDdlStmt() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL RESOURCE \"").append(name).append("\" PROPERTIES (");
        sb.append("\"type\" = \"ai_model\"");
        if (configs != null) {
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String key = entry.getKey();
                if (TYPE_KEY.equals(key)) {
                    continue;
                }
                String val = API_KEY.equals(key) ? API_KEY_MASK : entry.getValue().replace("\\", "\\\\").replace("\"", "\\\"");
                sb.append(", \"").append(key.replace("\\", "\\\\").replace("\"", "\\\""));
                sb.append("\" = \"").append(val).append("\"");
            }
        }
        sb.append(");");
        return sb.toString();
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        if (configs == null) {
            return;
        }
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String value = API_KEY.equals(entry.getKey()) ? API_KEY_MASK : entry.getValue();
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), value));
        }
    }

    @Override
    public Map<String, String> getProperties() {
        if (configs == null) {
            return Maps.newHashMap();
        }
        Map<String, String> masked = Maps.newHashMap(configs);
        if (masked.containsKey(API_KEY)) {
            masked.put(API_KEY, API_KEY_MASK);
        }
        return masked;
    }

    public String getEndpoint() {
        return configs == null ? null : configs.get(ENDPOINT);
    }

    public String getModel() {
        return configs == null ? null : configs.get(MODEL);
    }

    public String getApiKey() {
        return configs == null ? null : configs.get(API_KEY);
    }

    public String getProvider() {
        if (configs == null) {
            return DEFAULT_PROVIDER;
        }
        String provider = configs.get(PROVIDER);
        return StringUtils.isBlank(provider) ? DEFAULT_PROVIDER : provider;
    }

    public String getExtraParams() {
        if (configs == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (RESERVED_KEYS.contains(key)) {
                continue;
            }
            if (key.startsWith("ai_model.")) {
                key = key.substring("ai_model.".length());
                if (RESERVED_KEYS.contains(key)) {
                    continue;
                }
            }
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("\"").append(escapeJson(key)).append("\": ");
            sb.append(toJsonValue(entry.getValue()));
        }
        if (sb.length() == 0) {
            return "";
        }
        return "{" + sb + "}";
    }

    private static String toJsonValue(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return value.toLowerCase();
        }
        // Integer first: "42" → 42, not 42.0.
        // LLM APIs require integer values for params like max_tokens, top_k.
        try {
            long l = Long.parseLong(value);
            return new JsonPrimitive(l).toString();
        } catch (NumberFormatException ignored) {
        }
        try {
            double d = Double.parseDouble(value);
            if (Double.isFinite(d)) {
                return new JsonPrimitive(d).toString();
            }
        } catch (NumberFormatException ignored) {
        }
        // JSON array/object: pass through raw so that stop=["token"] stays an array.
        if (value.length() >= 2 &&
                ((value.startsWith("[") && value.endsWith("]")) ||
                 (value.startsWith("{") && value.endsWith("}")))) {
            try {
                JsonParser.parseString(value);
                return value;
            } catch (JsonSyntaxException ignored) {
            }
        }
        return new JsonPrimitive(value).toString();
    }

    // Delegates to Gson for RFC 8259 compliant JSON string escaping.
    private static String escapeJson(String s) {
        String quoted = new JsonPrimitive(s).toString();
        return quoted.substring(1, quoted.length() - 1);
    }
}
