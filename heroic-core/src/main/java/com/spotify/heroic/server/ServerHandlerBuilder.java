package com.spotify.heroic.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.ResourceConfigurator;
import com.spotify.heroic.lib.httpcore.MimeType;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsRoutesProvider;
import io.norberg.rut.Router;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class ServerHandlerBuilder implements ResourceConfigurator {
    private static final Joiner PATH_JOINER = Joiner.on('/');

    private final ObjectMapper mapper;
    private final ImmutableList.Builder<RequestFilter> filters = ImmutableList.builder();
    private final ImmutableList.Builder<RsMapping<AsyncFuture<?>>> mappings =
        ImmutableList.builder();

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

    public CoreServerInstance build() {
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
                .map(this::toMimeTarget);

            final List<MimeTypeRoute> routes = new ArrayList<>();

            for (final RsMapping<AsyncFuture<?>> mapping : e.getValue()) {
                for (final String mimeType : new HashSet<>(mapping.consumes())) {
                    routes.add(new MimeTypeRoute(MimeType.parse(mimeType), toMimeTarget(mapping)));
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

        return new CoreServerInstance(filters.build(), deserializers, serializers,
            router.build());
    }

    private RouteTarget toMimeTarget(final RsMapping<AsyncFuture<?>> mapping) {
        final List<MimeType> produces =
            mapping.produces().stream().map(MimeType::parse).collect(Collectors.toList());

        return new RouteTarget(mapping, produces);
    }

    private Deserializers setupDeserializers() {
        final ImmutableList.Builder<MimeTypeDeserializer> deserializers = ImmutableList.builder();

        deserializers.add(
            new MimeTypeDeserializer(MimeType.parse("application/json"), this::deserializeJson));
        return new Deserializers(deserializers.build());
    }

    private Serializers setupSerializers() {
        final ImmutableMap.Builder<MimeType, Function<SerializeContext, ByteBuffer>> serializers =
            ImmutableMap.builder();

        serializers.put(MimeType.parse("application/json"), this::serializeJson);
        return new Serializers(serializers.build());
    }

    private Object deserializeJson(final DeserializeContext context) {
        final ByteBuffer entity = context.getEntity();
        final byte[] bytes = new byte[entity.remaining()];
        entity.get(bytes);

        try {
            return mapper.readValue(bytes, new TypeReference<Object>() {
                @Override
                public Type getType() {
                    return context.getType();
                }
            });
        } catch (final IOException e) {
            throw new HandlerException(e);
        }
    }

    private ByteBuffer serializeJson(final SerializeContext context) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(context.getEntity()));
        } catch (final JsonProcessingException e) {
            throw new HandlerException(e);
        }
    }

    @Data
    static class MimeTypeRoute {
        private final MimeType mimeType;
        private final RouteTarget target;
    }

    @Data
    static class RouteKey {
        private final String method;
        private final String path;
    }
}
