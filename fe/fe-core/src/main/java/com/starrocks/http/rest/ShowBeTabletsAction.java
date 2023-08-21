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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/CheckDecommissionAction.java

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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

/*
 * fe_host:fe_http_port/api/show_be_tablets?host_port=be_host:heartbeat_port
 * return:
 * {"status":"OK","isDecommissioned":"true/false","tabletCount":"123","tabletIds":"[1,2,3]"}
 * {"status":"FAILED","msg":"err info..."}
 */
public class ShowBeTabletsAction extends RestBaseAction {
    public static final String HOST_PORT = "host_port";

    public ShowBeTabletsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_be_tablets", new ShowBeTabletsAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkActionOnSystem(currentUser, PrivilegeType.OPERATE);

        String hostPort = request.getSingleParameter(HOST_PORT);
        if (Strings.isNullOrEmpty(hostPort)) {
            throw new DdlException("Need to specify host and port of BE.");
        }

        Pair<String, Integer> pair;
        try {
            pair = SystemInfoService.validateHostAndPort(hostPort, false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithHeartbeatPort(pair.first, pair.second);
        if (backend == null) {
            throw new DdlException("Backend (" + pair.first + ":" + pair.second +  ") does not exist.");
        }

        List<Long> backendTabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId());
        int tabletCount = backendTabletIds.size();

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("status", "OK");
        jsonObject.addProperty("isDecommissioned", backend.isDecommissioned());
        jsonObject.addProperty("tabletCount", tabletCount);
        jsonObject.addProperty("tabletIds", new Gson().toJson(backendTabletIds));

        // send result
        response.setContentType("application/json");
        response.getContent().append(jsonObject);
        sendResult(request, response);
    }
}
