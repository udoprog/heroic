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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.ResourceConfigurator;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.jetty.ServerConnector;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.Observer;
import com.spotify.heroic.server.OnceObservable;
import com.spotify.heroic.server.ServerHandle;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerResponse;
import com.spotify.heroic.server.ServerSetup;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.rs.BuiltinRsParameter;
import eu.toolchain.rs.RsFunction;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsParameter;
import eu.toolchain.rs.RsRequestContext;
import eu.toolchain.rs.RsRoutesProvider;
import io.norberg.rut.Router;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MediaType;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

@HttpServerScope
@Slf4j
@ToString(of = {"address"})
public class HttpServer implements LifeCycles {
    public static final String DEFAULT_CORS_ALLOW_ORIGIN = "*";

    private final List<ServerSetup> servers;
    private final InetSocketAddress address;
    private final HeroicCoreInstance instance;
    private final HeroicConfigurationContext config;
    private final ObjectMapper mapper;
    private final AsyncFramework async;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final List<ServerConnector> connectors;
    private final Supplier<Boolean> stopping;

    private volatile List<ServerHandle> handles = null;
    private final Object lock = new Object();

    @Inject
    public HttpServer(
        List<ServerSetup> servers, @Named("bind") final InetSocketAddress address,
        final HeroicCoreInstance instance, final HeroicConfigurationContext config,
        @Named(MediaType.APPLICATION_JSON) final ObjectMapper mapper, final AsyncFramework async,
        @Named("enableCors") final boolean enableCors,
        @Named("corsAllowOrigin") final Optional<String> corsAllowOrigin,
        final List<ServerConnector> connectors, @Named("stopping") final Supplier<Boolean> stopping
    ) {
        this.servers = servers;
        this.address = address;
        this.instance = instance;
        this.config = config;
        this.mapper = mapper;
        this.async = async;
        this.enableCors = enableCors;
        this.corsAllowOrigin = corsAllowOrigin;
        this.connectors = connectors;
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
        final ServerHandlerBuilder builder = new ServerHandlerBuilder();

        setupResources(builder);
        setupRewrites(builder);
        /* TODO: request logger */

        final ServerHandler serverHandler = builder.build();

        final List<AsyncFuture<ServerHandle>> handlers = servers
            .stream()
            .map(server -> server.bind(address, serverHandler))
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
            configurator.addResponseFilter(
                new CorsResponseFilter(corsAllowOrigin.orElse(DEFAULT_CORS_ALLOW_ORIGIN)));
        }
    }

    @RequiredArgsConstructor
    class ServerHandler implements ServerInstance {
        private final Router<RsMapping<AsyncFuture<?>>> router;

        public void start() {
        }

        public void stop() {
        }

        @Override
        public OnceObservable<Observer<ByteBuffer>> call(
            final String method, final String path, final Headers headers,
            final Consumer<OnceObservable<ServerResponse>> response
        ) {
            final Router.Result<RsMapping<AsyncFuture<?>>> result = router.result();
            router.route(method, path, result);

            if (!result.isSuccess()) {
                response.accept(observer -> {
                    observer.observe(ServerResponse.notFound());
                });

                /* The stateful aspect of the request handling is unfortunate. */
                return requestBody -> {
                    /* do nothing?, this should prevent resources associated with the request body
                     * to never be allocated */
                };
            }

            final Map<String, String> pathParams = new HashMap<>();

            for (int i = 0; i < result.params(); i++) {
                pathParams.put(result.paramName(i), result.paramValueDecoded(i).toString());
            }

            RsMapping<AsyncFuture<?>> target = result.target();

            final RsFunction<RsRequestContext, AsyncFuture<?>> handle = target.handle();

            /**
             * Request body is received in chunks, modelled through an observer here.
             */
            return requestBody -> {
                requestBody.observe(new Observer<ByteBuffer>() {
                    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                    @Override
                    public void observe(final ByteBuffer bytes) {
                        outputStream.write(bytes.array(), bytes.arrayOffset(), bytes.remaining());
                    }

                    @Override
                    public void abort(final Throwable reason) {
                        try {
                            outputStream.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void end() {
                        response.accept(observer -> {
                            final AsyncFuture<?> data;

                            try {
                                data = handle.apply(new RequestContext(result, pathParams));
                            } catch (Exception e) {
                                observer.fail(e);
                                return;
                            }

                            data
                                .directTransform(mapper::writeValueAsBytes)
                                .onDone(new FutureDone<byte[]>() {
                                    @Override
                                    public void resolved(final byte[] bytes) throws Exception {
                                        observer.observe(ServerResponse.ok(ByteBuffer.wrap(bytes)));
                                    }

                                    @Override
                                    public void failed(final Throwable cause) throws Exception {
                                        observer.fail(cause);
                                    }

                                    @Override
                                    public void cancelled() throws Exception {
                                        observer.fail(new RuntimeException("cancelled"));
                                    }
                                });
                        });
                    }
                });
            };
        }
    }

    private static final Joiner PATH_JOINER = Joiner.on('/');

    @RequiredArgsConstructor
    class RequestContext implements RsRequestContext {
        private final Router.Result<RsMapping<AsyncFuture<?>>> result;
        private final Map<String, String> pathParams;

        @Override
        public Optional<RsParameter> getPathParameter(final String key) {
            return Optional.ofNullable(pathParams.get(key)).map(StringParameter::new);
        }

        @Override
        public Stream<RsParameter> getAllQueryParameters(final String key) {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public Optional<RsParameter> getQueryParameter(final String key) {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public Stream<RsParameter> getAllHeaderParameters(final String key) {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public Optional<RsParameter> getHeaderParameter(final String key) {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public Optional<RsParameter> getPayload() {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public <T> T getContext(final Class<T> type) {
            throw new NotImplementedException("not implemented");
        }

        @Override
        public Supplier<RsParameter> provideDefault(final String defaultValue) {
            throw new NotImplementedException("not implemented");
        }
    }

    class ServerHandlerBuilder implements ResourceConfigurator {
        final ImmutableList.Builder<RsMapping<AsyncFuture<?>>> mappings = ImmutableList.builder();

        @Override
        public void addResponseFilter(final ContainerResponseFilter responseFilter) {
            /* TODO: setup response filter, possibly a simpler implementation? */
        }

        @Override
        public void addSimpleRewrite(final String fromPrefix, final String toPrefix) {
            /* TODO: setup simple rewrite */
        }

        @Override
        public void addRoutes(
            final RsRoutesProvider<? extends RsMapping<? extends AsyncFuture<?>>> provider
        ) {
            provider.routes().forEach(mapping -> {
                mappings.add((RsMapping<AsyncFuture<?>>) mapping);
            });
        }

        public ServerHandler build() {
            final Router.Builder<RsMapping<AsyncFuture<?>>> router = Router.builder();

            final ImmutableList<RsMapping<AsyncFuture<?>>> mappingsList = mappings.build();

            for (final RsMapping<AsyncFuture<?>> mapping : mappingsList) {
                router.route(mapping.method(), PATH_JOINER.join(mapping.path()), mapping);
            }

            return new ServerHandler(router.build());
        }
    }

    @RequiredArgsConstructor
    private static class StringParameter extends BuiltinRsParameter {
        private final String source;

        @Override
        public RuntimeException conversionError(
            final String reason, final Object source, final Throwable cause
        ) {
            return new RuntimeException(reason, cause);
        }

        @Override
        public String asString() {
            return source;
        }

        @Override
        public <T> T asType(final Class<T> type) {
            throw new RuntimeException("Cannot convert to type: " + type);
        }
    }
}
