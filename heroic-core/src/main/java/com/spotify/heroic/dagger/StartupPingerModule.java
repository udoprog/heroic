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

package com.spotify.heroic.dagger;

import com.spotify.heroic.HeroicStartupPinger;
import com.spotify.heroic.server.ServerManager;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.net.URI;
import java.util.Optional;

@RequiredArgsConstructor
@Module
public class StartupPingerModule {
    private final URI ping;
    private final String id;
    private final Optional<ServerManager> server;

    @Provides
    @StartupPingerScope
    Optional<ServerManager> server() {
        return server;
    }

    @Provides
    @StartupPingerScope
    @Named("pingURI")
    URI ping() {
        return ping;
    }

    @Provides
    @StartupPingerScope
    @Named("pingId")
    String id() {
        return id;
    }

    @Provides
    @StartupPingerScope
    @Named("startupPinger")
    LifeCycle startupPingerLife(LifeCycleManager manager, HeroicStartupPinger pinger) {
        return manager.build(pinger);
    }
}
