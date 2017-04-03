/*
 * Copyright (c) 2017 Spotify AB.
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

import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.server.RequestContext;
import java.util.Optional;

public class CoreHttpContextFactory {
    public static HttpContext create(final RequestContext requestContext) {
        final Optional<String> xForwardedFor = requestContext.getHeader("X-Forwarded-For");
        final String remoteAddress = requestContext.getRemoteAddress();
        final String clientAddress = xForwardedFor.orElse(remoteAddress);
        final Optional<String> userAgent = requestContext.getHeader("User-Agent");
        final Optional<String> clientId = requestContext.getHeader("X-Client-Id");
        return new HttpContext(remoteAddress, requestContext.getRemoteHost(), clientAddress,
            userAgent, clientId);
    }
}
