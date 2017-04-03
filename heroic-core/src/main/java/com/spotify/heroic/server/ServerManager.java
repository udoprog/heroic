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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.ResourceConfigurator;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.function.ThrowingFunction;
import com.spotify.heroic.lib.httpcore.Accept;
import com.spotify.heroic.lib.httpcore.ContentType;
import com.spotify.heroic.lib.httpcore.MimeType;
import com.spotify.heroic.lib.httpcore.Status;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.rs.BuiltinRsParameter;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsParameter;
import eu.toolchain.rs.RsRequestContext;
import eu.toolchain.rs.RsRoutesProvider;
import eu.toolchain.rs.RsTypeReference;
import io.norberg.rut.Router;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ServerManagerScope
@Slf4j
@ToString(of = {"address"})
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
        final ServerHandlerBuilder builder = new ServerHandlerBuilder();

        setupResources(builder);
        setupRewrites(builder);
        /* TODO: request logger */

        final ServerHandler serverHandler = builder.build();

        final List<AsyncFuture<ServerHandle>> handlers = servers
            .stream()
            .map(server -> server.bind(defaultHost, defaultPort, serverHandler))
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

    @RequiredArgsConstructor
    class ServerHandler implements ServerInstance {
        private final List<RequestFilter> filters;
        private final Deserializers deserializers;
        private final Serializers serializers;
        private final Router<RouteEntry> router;

        public void start() {
        }

        public void stop() {
        }

        @Override
        public BodyWriter call(
            final ServerRequest request, final OnceObserver<ServerResponse> response
        ) {
            if (filters.isEmpty()) {
                return doCall(request, response);
            }

            BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> current =
                this::doCall;

            for (final RequestFilter requestFilter : filters) {
                final BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> next =
                    current;
                current = (req, res) -> requestFilter.filter(request, response, next);
            }

            return current.apply(request, response);
        }

        private BodyWriter doCall(
            final ServerRequest request, final OnceObserver<ServerResponse> response
        ) {
            final String method = request.method();
            final String path = request.path();
            final Headers headers = request.headers();

            final Router.Result<RouteEntry> result = router.result();
            router.route(request.method(), request.path(), result);

            final Optional<RouteEntry> entry =
                result.isSuccess() ? Optional.of(result.target()) : Optional.empty();

            final Optional<RouteTarget> optionalTarget = entry.flatMap(e -> e.resolve(headers));

            if (!optionalTarget.isPresent()) {
                throw new ProcessingException(Status.NOT_FOUND);
            }

            final RouteTarget target = optionalTarget.get();

            final Map<String, String> pathParams = new HashMap<>();

            for (int i = 0; i < result.params(); i++) {
                pathParams.put(result.paramName(i), result.paramValueDecoded(i).toString());
            }

            final int size = headers.contentLength().orElse(0);
            final ByteBuffer input = ByteBuffer.allocate(size);

            /**
             * Request body is received in chunks, modelled through an observer here.
             */
            return new RequestObserver(size, method, path, headers, target, pathParams,
                deserializers, serializers, input, response);
        }
    }

    private static final Joiner PATH_JOINER = Joiner.on('/');

    @Data
    static class RequestObserver implements BodyWriter {
        private final int size;
        private final String method;
        private final String path;
        private final Headers headers;
        private final RouteTarget target;
        private final Map<String, String> pathParams;
        private final Deserializers deserializers;
        private final Serializers serializers;
        private final ByteBuffer input;
        private final OnceObserver<ServerResponse> observer;

        boolean failed = false;

        @Override
        public void write(final ByteBuffer bytes) {
            if (input.position() + bytes.remaining() > size) {
                throw new ProcessingException(Status.EXPECTATION_FAILED);
            }

            input.put(bytes);
        }

        @Override
        public void end() {
            if (failed) {
                log.warn("request observer has already reported failure");
                return;
            }

            input.flip();

            final Optional<ByteBuffer> entity =
                input.remaining() == 0 ? Optional.empty() : Optional.of(input);

            final RsMapping<AsyncFuture<?>> mapping = target.getMapping();

            final AsyncFuture<?> data;

            try {
                data = mapping
                    .handle()
                    .apply(new RequestContext(entity, method, path, headers, mapping, pathParams,
                        deserializers));
            } catch (final Exception e) {
                throw new HandlerException(e);
            }

            data
                .directTransform(this::responseFilter)
                .directTransform(this::serverResponseFilter)
                .onDone(new FutureDone<ServerResponse>() {
                    @Override
                    public void resolved(final ServerResponse response) throws Exception {
                        observer.observe(response);
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
        }

        private ServerResponse serverResponseFilter(final Response response) {
            return new ImmediateServerResponse(response.getStatus(), response.getHeaders(),
                response.getEntity().map(entity -> this.serializeEntity(response, entity)));
        }

        private ByteBuffer serializeEntity(final Response response, final Object entity) {
            if (entity instanceof ByteBuffer) {
                return (ByteBuffer) entity;
            }

            if (entity instanceof byte[]) {
                return ByteBuffer.wrap((byte[]) entity);
            }

            final List<MimeType> produces = target.getProduces();

            if (produces.isEmpty()) {
                throw new ProcessingException(Status.INTERNAL_SERVER_ERROR,
                    "@Produces missing on Endpoint");
            }

            final Accept accept = headers.accept().orElse(Accept.ANY);

            final MimeType match = accept
                .matchBest(produces)
                .orElseThrow(() -> new ProcessingException(Status.UNSUPPORTED_MEDIA_TYPE,
                    "Could not match Accept (" + accept + ") to any known endpoint"));

            final ThrowingFunction<SerializeContext, ByteBuffer> serializer = serializers
                .match(match)
                .orElseThrow(() -> new ProcessingException(Status.UNSUPPORTED_MEDIA_TYPE));

            try {
                return serializer.apply(new SerializeContext(response, entity));
            } catch (Exception e) {
                throw new ProcessingException(Status.INTERNAL_SERVER_ERROR,
                    "Failed to serialize response");
            }
        }

        private Response responseFilter(final Object data) {
            if (data instanceof Response) {
                return (Response) data;
            }

            return Response.ok(data);
        }
    }

    @RequiredArgsConstructor
    static class RequestContext implements RsRequestContext {
        private final Optional<ByteBuffer> entity;
        private final String method;
        private final String path;
        private final Headers headers;
        private final RsMapping<AsyncFuture<?>> mapping;
        private final Map<String, String> pathParams;
        private final Deserializers deserializers;

        @Override
        public Optional<RsParameter> getPathParameter(final String key) {
            return Optional.ofNullable(pathParams.get(key)).map(StringParameter::new);
        }

        @Override
        public Stream<RsParameter> getAllQueryParameters(final String key) {
            throw notImplemented();
        }

        @Override
        public Optional<RsParameter> getQueryParameter(final String key) {
            throw notImplemented();
        }

        @Override
        public Stream<RsParameter> getAllHeaderParameters(final String key) {
            throw notImplemented();
        }

        @Override
        public Optional<RsParameter> getHeaderParameter(final String key) {
            throw notImplemented();
        }

        @Override
        public Optional<RsParameter> getPayload() {
            // TODO: do not assume UTF-8, read it from the Content-Type
            return entity.map(
                buffer -> new DeserializeRsParameter(buffer, StandardCharsets.UTF_8, method, path,
                    headers, mapping, deserializers));
        }

        @Override
        public <T> T getContext(final Class<T> type) {
            throw notImplemented();
        }

        @Override
        public Supplier<RsParameter> provideDefault(final String defaultValue) {
            throw notImplemented();
        }

        private ProcessingException notImplemented() {
            return new ProcessingException(Status.INTERNAL_SERVER_ERROR, "Not implemented");
        }
    }

    @Data
    static class DeserializeRsParameter extends BuiltinRsParameter {
        private final ByteBuffer buffer;
        private final Charset charset;
        private final String method;
        private final String path;
        private final Headers headers;
        private final RsMapping<AsyncFuture<?>> mapping;
        private final Deserializers deserializers;

        @Override
        public String asString() {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, charset);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T asType(final RsTypeReference<T> type) {
            final ContentType contentType = headers
                .contentType()
                .orElseThrow(
                    () -> new ProcessingException(Status.BAD_REQUEST, "Content-Type Missing"));

            final ThrowingFunction<DeserializeContext, Object> fn = deserializers
                .match(contentType)
                .orElseThrow(() -> new ProcessingException(Status.BAD_REQUEST,
                    "Unsupported Content-Type (" + contentType + ")"));

            try {
                return (T) fn.apply(
                    new DeserializeContext(method, path, headers, mapping, buffer, type.getType()));
            } catch (Exception e) {
                throw new HandlerException(e);
            }
        }

        @Override
        public RuntimeException conversionError(
            final String reason, final Object source, final Throwable cause
        ) {
            return new RuntimeException(reason, cause);
        }
    }

    class ServerHandlerBuilder implements ResourceConfigurator {
        final ImmutableList.Builder<RequestFilter> filters = ImmutableList.builder();
        final ImmutableList.Builder<RsMapping<AsyncFuture<?>>> mappings = ImmutableList.builder();

        @Override
        public void addRequestFilter(final RequestFilter requestFilter) {
            filters.add(requestFilter);
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
            final Multimap<RouteKey, RsMapping<AsyncFuture<?>>> map = HashMultimap.create();

            for (final RsMapping<AsyncFuture<?>> mapping : mappings.build()) {
                map.put(new RouteKey(mapping.method(), "/" + PATH_JOINER.join(mapping.path())),
                    mapping);
            }

            final Router.Builder<RouteEntry> router = Router.builder();

            for (final Map.Entry<RouteKey, Collection<RsMapping<AsyncFuture<?>>>> e : map
                .asMap()
                .entrySet()) {

                Optional<RouteTarget> defaultMapping = e
                    .getValue()
                    .stream()
                    .filter(mapping -> mapping.consumes().isEmpty())
                    .findFirst()
                    .map(ServerManager.this::toMimeTarget);

                final List<MimeTypeRoute> routes = new ArrayList<>();

                for (final RsMapping<AsyncFuture<?>> mapping : e.getValue()) {
                    for (final String mimeType : new HashSet<>(mapping.consumes())) {
                        routes.add(
                            new MimeTypeRoute(MimeType.parse(mimeType), toMimeTarget(mapping)));
                    }
                }

                if (!defaultMapping.isPresent() && routes.size() == 1) {
                    defaultMapping = Optional.of(routes.iterator().next().getTarget());
                }

                router.route(e.getKey().getMethod(), e.getKey().getPath(),
                    new RouteEntry(defaultMapping, routes));
            }

            final Deserializers deserializers = setupDeserializers();
            final Serializers serializers = setupSerializers();

            return new ServerHandler(filters.build(), deserializers, serializers, router.build());
        }
    }

    private Deserializers setupDeserializers() {
        final ImmutableList.Builder<MimeTypeDeserializer> deserializers = ImmutableList.builder();

        deserializers.add(new MimeTypeDeserializer(MimeType.parse("application/json"),
            ServerManager.this::deserializeJson));
        return new Deserializers(deserializers.build());
    }

    private Serializers setupSerializers() {
        final ImmutableMap.Builder<MimeType, ThrowingFunction<SerializeContext, ByteBuffer>>
            serializers = ImmutableMap.builder();

        serializers.put(MimeType.parse("application/json"), ServerManager.this::serializeJson);

        return new Serializers(serializers.build());
    }

    private RouteTarget toMimeTarget(final RsMapping<AsyncFuture<?>> mapping) {
        final List<MimeType> produces =
            mapping.produces().stream().map(MimeType::parse).collect(Collectors.toList());

        return new RouteTarget(mapping, produces);
    }

    private Object deserializeJson(final DeserializeContext context) throws Exception {
        final ByteBuffer entity = context.getEntity();
        final byte[] bytes = new byte[entity.remaining()];
        entity.get(bytes);

        return mapper.readValue(bytes, new TypeReference<Object>() {
            @Override
            public Type getType() {
                return context.getType();
            }
        });
    }

    private ByteBuffer serializeJson(final SerializeContext context) throws Exception {
        return ByteBuffer.wrap(mapper.writeValueAsBytes(context.getEntity()));
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
        public <T> T asType(final RsTypeReference<T> type) {
            throw new RuntimeException("Cannot convert to type: " + type.getType());
        }
    }

    @Data
    private static class RouteKey {
        private final String method;
        private final String path;
    }

    @Data
    private static class RouteEntry {
        private final Optional<RouteTarget> defaultMapping;
        private final List<MimeTypeRoute> routes;

        public Optional<RouteTarget> resolve(final Headers headers) {
            return headers.contentType().map(this::matchRoute).orElse(defaultMapping);
        }

        private Optional<RouteTarget> matchRoute(final ContentType contentType) {
            for (final MimeTypeRoute route : routes) {
                if (route.mimeType.matches(contentType.getMimeType())) {
                    return Optional.of(route.target);
                }
            }

            return Optional.empty();
        }
    }

    @Data
    private static class MimeTypeRoute {
        private final MimeType mimeType;
        private final RouteTarget target;
    }

    @Data
    private static class RouteTarget {
        private final RsMapping<AsyncFuture<?>> mapping;
        private final List<MimeType> produces;
    }

    @Data
    private static class DeserializeContext {
        private final String method;
        private final String path;
        private final Headers headers;
        private final RsMapping<?> mapping;
        private final ByteBuffer entity;
        private final Type type;
    }

    @Data
    private static class SerializeContext {
        private final Response response;
        private final Object entity;
    }

    @Data
    private static class Deserializers {
        private final List<MimeTypeDeserializer> deserializers;

        public Optional<ThrowingFunction<DeserializeContext, Object>> match(
            final ContentType contentType
        ) {
            final MimeType mime = contentType.getMimeType();

            for (final MimeTypeDeserializer deserializer : deserializers) {
                if (mime.matches(deserializer.mimeType)) {
                    return Optional.of(deserializer.deserializer);
                }
            }

            return Optional.empty();
        }
    }

    @Data
    private static class MimeTypeDeserializer {
        private final MimeType mimeType;
        private final ThrowingFunction<DeserializeContext, Object> deserializer;
    }

    @Data
    private static class Serializers {
        private final Map<MimeType, ThrowingFunction<SerializeContext, ByteBuffer>> serializers;

        public Optional<ThrowingFunction<SerializeContext, ByteBuffer>> match(final MimeType mime) {
            return Optional.ofNullable(serializers.get(mime));
        }
    }
}
