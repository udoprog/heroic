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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.ResourceConfigurator;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.server.filter.CorsRequestFilter;
import com.spotify.heroic.server.filter.JsonErrorRequestFilter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@ServerManagerScope
@Slf4j
public class ServerManager implements LifeCycles {
    public static final String DEFAULT_CORS_ALLOW_ORIGIN = "*";

    private final List<ServerSetup> servers;
    private final String defaultHost;
    private final Integer defaultPort;
    private final HeroicCoreInstance instance;
    private final HeroicConfigurationContext config;
    private final ObjectMapper mapper;
    private final AsyncFramework async;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final Supplier<Boolean> stopping;

    private volatile List<ServerHandle> handles = null;
    private final Object lock = new Object();

    @Inject
    public ServerManager(
        List<ServerSetup> servers, @Named("defaultHost") final String defaultHost,
        @Named("defaultPort") final Integer defaultPort, final HeroicCoreInstance instance,
        final HeroicConfigurationContext config,
        @Named(MediaType.APPLICATION_JSON) final ObjectMapper mapper, final AsyncFramework async,
        @Named("enableCors") final boolean enableCors,
        @Named("corsAllowOrigin") final Optional<String> corsAllowOrigin,
        @Named("stopping") final Supplier<Boolean> stopping
    ) {
        this.servers = servers;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        this.instance = instance;
        this.config = config;
        this.mapper = mapper;
        this.async = async;
        this.enableCors = enableCors;
        this.corsAllowOrigin = corsAllowOrigin;
        this.stopping = stopping;
    }

    @Override
    public void register(final LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    public int getPort() {
        throw new IllegalStateException("Server is not running");
    }

    private AsyncFuture<Void> start() {
        final ServerHandlerBuilder builder = new ServerHandlerBuilder(mapper);

        setupResources(builder);
        setupRewrites(builder);
        /* TODO: request logger */

        final CoreServerInstance coreServerInstance = builder.build();

        final List<AsyncFuture<ServerHandle>> handlers = servers
            .stream()
            .map(server -> server.bind(defaultHost, defaultPort, coreServerInstance))
            .collect(Collectors.toList());

        return async.collect(handlers).directTransform(handles -> {
            synchronized (lock) {
                if (this.handles != null) {
                    throw new IllegalStateException("server already started");
                }

                this.handles = ImmutableList.copyOf(handles);
                return null;
            }
        });
    }

    private AsyncFuture<Void> stop() {
        final List<ServerHandle> handles;

        synchronized (lock) {
            if (this.handles == null) {
                throw new IllegalStateException("Server has not been started");
            }

            handles = this.handles;
            this.handles = null;
        }

        return async.collectAndDiscard(
            handles.stream().map(ServerHandle::stop).collect(Collectors.toList()));
    }

    private void setupRewrites(ResourceConfigurator configurator) {
        configurator.addSimpleRewrite("/metrics", "/query/metrics");
        configurator.addSimpleRewrite("/metrics-stream/*", "/query/metrics-stream");
        configurator.addSimpleRewrite("/tags", "/metadata/tags");
        configurator.addSimpleRewrite("/keys", "/metadata/keys");
    }

    private void setupResources(ResourceConfigurator configurator) {
        for (final Function<CoreComponent, Consumer<ResourceConfigurator>> resource : config
            .getResources()) {
            if (log.isTraceEnabled()) {
                log.trace("Loading resource: {}", resource);
            }

            instance.inject(resource).accept(configurator);
        }

        // Resources.
        if (enableCors) {
            configurator.addRequestFilter(
                new CorsRequestFilter(corsAllowOrigin.orElse(DEFAULT_CORS_ALLOW_ORIGIN)));
        }

        configurator.addRequestFilter(new JsonErrorRequestFilter(mapper));
    }
}
