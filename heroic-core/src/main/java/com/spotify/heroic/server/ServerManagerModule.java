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

package com.spotify.heroic.server;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import dagger.Provides;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@dagger.Module
public class ServerManagerModule {
    private final String defaultHost;
    private final Integer defaultPort;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final List<ServerModule> servers;

    @Provides
    @Named("defaultHost")
    @ServerManagerScope
    String defaultHost() {
        return defaultHost;
    }

    @Provides
    @Named("defaultPort")
    @ServerManagerScope
    Integer defaultPort() {
        return defaultPort;
    }

    @Provides
    @Named("enableCors")
    @ServerManagerScope
    boolean enableCors() {
        return enableCors;
    }

    @Provides
    @Named("corsAllowOrigin")
    @ServerManagerScope
    Optional<String> corsAllowOrigin() {
        return corsAllowOrigin;
    }

    @Provides
    @ServerManagerScope
    @Named("heroicServer")
    LifeCycle life(LifeCycleManager manager, ServerManager serverManager) {
        return manager.build(serverManager);
    }

    @Provides
    @ServerManagerScope
    List<ServerSetup> servers(final PrimaryComponent primary) {
        return servers.stream().map(server -> server.module(primary)).collect(Collectors.toList());
    }
}
