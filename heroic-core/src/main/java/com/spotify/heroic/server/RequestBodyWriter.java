package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Accept;
import com.spotify.heroic.lib.httpcore.ContentType;
import com.spotify.heroic.lib.httpcore.MimeType;
import com.spotify.heroic.lib.httpcore.Status;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.rs.BuiltinRsParameter;
import eu.toolchain.rs.RsMapping;
import eu.toolchain.rs.RsParameter;
import eu.toolchain.rs.RsRequestContext;
import eu.toolchain.rs.RsTypeReference;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class RequestBodyWriter implements BodyWriter {
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

        final RequestContext requestContext =
            new RequestContext(entity, method, path, headers, mapping, pathParams, deserializers);

        try {
            data = mapping.handle().apply(requestContext);
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
            response.getEntity().map(entity -> serializeEntity(response, entity)));
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

        final Function<SerializeContext, ByteBuffer> serializer = serializers
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
            return entity.map(
                buffer -> new DeserializeRsParameter(buffer, method, path, headers, mapping,
                    deserializers));
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

    @RequiredArgsConstructor
    static class StringParameter extends BuiltinRsParameter {
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
    static class DeserializeRsParameter extends BuiltinRsParameter {
        private final ByteBuffer buffer;
        private final String method;
        private final String path;
        private final Headers headers;
        private final RsMapping<AsyncFuture<?>> mapping;
        private final Deserializers deserializers;

        @Override
        public String asString() {
            final ContentType contentType = headers
                .contentType()
                .orElseThrow(
                    () -> new ProcessingException(Status.BAD_REQUEST, "Content-Type Missing"));

            // fallback to UTF-8
            final Charset charset = contentType.getCharset().orElse(StandardCharsets.UTF_8);

            try {
                return charset.decode(buffer).toString();
            } catch (final Exception e) {
                throw new ProcessingException(Status.BAD_REQUEST,
                    "Entity does not match expected encoding " + charset);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T asType(final RsTypeReference<T> type) {
            final ContentType contentType = headers
                .contentType()
                .orElseThrow(
                    () -> new ProcessingException(Status.BAD_REQUEST, "Content-Type Missing"));

            final Function<DeserializeContext, Object> fn = deserializers
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
}
