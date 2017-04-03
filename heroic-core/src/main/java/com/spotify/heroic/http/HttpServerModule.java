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

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.jetty.ServerConnector;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.server.ServerModule;
import com.spotify.heroic.server.ServerSetup;
import dagger.Provides;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@dagger.Module
public class HttpServerModule {
    private final InetSocketAddress bind;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final List<ServerConnector> connectors;
    private final List<ServerModule> servers;

    @Provides
    @Named("bind")
    @HttpServerScope
    InetSocketAddress bind() {
        return bind;
    }

    @Provides
    @Named("enableCors")
    @HttpServerScope
    boolean enableCors() {
        return enableCors;
    }

    @Provides
    @Named("corsAllowOrigin")
    @HttpServerScope
    Optional<String> corsAllowOrigin() {
        return corsAllowOrigin;
    }

    @Provides
    @HttpServerScope
    List<ServerConnector> connectors() {
        return connectors;
    }

    @Provides
    @HttpServerScope
    @Named("heroicServer")
    LifeCycle life(LifeCycleManager manager, HttpServer server) {
        return manager.build(server);
    }

    @Provides
    @HttpServerScope
    List<ServerSetup> servers(final PrimaryComponent primary) {
        return servers.stream().map(server -> server.module(primary)).collect(Collectors.toList());
    }
}
