/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http;

import lombok.Data;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

@Data
public class CorsResponseFilter implements ContainerResponseFilter {
    private final String corsAllowOrigin;

    public void filter(
        ContainerRequestContext requestContext, ContainerResponseContext responseContext
    ) throws IOException {
        final MultivaluedMap<String, Object> headers = responseContext.getHeaders();

        if (requestContext.getMethod().equalsIgnoreCase("OPTIONS")) {
            final String request = requestContext.getHeaderString("Access-Control-Request-Headers");
            final String method = requestContext.getHeaderString("Access-Control-Request-Method");

            if (request == null || method == null) {
                return;
            }

            headers.add("Access-Control-Allow-Origin", corsAllowOrigin);
            headers.add("Access-Control-Allow-Methods", method);
            headers.add("Access-Control-Allow-Headers", request);
        } else {
            headers.add("Access-Control-Allow-Origin", corsAllowOrigin);
        }
    }
}
