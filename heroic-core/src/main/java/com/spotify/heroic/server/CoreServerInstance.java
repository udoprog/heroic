package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.ContentType;
import com.spotify.heroic.lib.httpcore.Status;
import io.norberg.rut.Router;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class CoreServerInstance implements ServerInstance {
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

        BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> current = this::doCall;

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
        final RouteTarget target = handleRouting(request, headers, result);

        final Map<String, String> pathParams = new HashMap<>();

        for (int i = 0; i < result.params(); i++) {
            pathParams.put(result.paramName(i), result.paramValueDecoded(i).toString());
        }

        final int size = headers.contentLength().orElse(0);
        final ByteBuffer input = ByteBuffer.allocate(size);

        // Request body is received in chunks, modelled through an observer here.
        return new RequestBodyWriter(size, method, path, headers, target, pathParams, deserializers,
            serializers, input, response);
    }

    private RouteTarget handleRouting(
        final ServerRequest request, final Headers headers, final Router.Result<RouteEntry> result
    ) {
        router.route(request.method(), request.path(), result);

        switch (result.status()) {
            case SUCCESS:
                break;
            case NOT_FOUND:
                throw new ProcessingException(Status.NOT_FOUND);
            case METHOD_NOT_ALLOWED:
                throw new ProcessingException(Status.METHOD_NOT_ALLOWED);
        }

        final Optional<ContentType> contentType = headers.contentType();

        final RouteEntry entry = result.target();

        return entry.resolve(contentType).orElseThrow(() -> {
            if (contentType.isPresent()) {
                return new ProcessingException(Status.UNSUPPORTED_MEDIA_TYPE,
                    "Unsupported Content-Type: " + contentType.get());
            }

            return new ProcessingException(Status.UNSUPPORTED_MEDIA_TYPE);
        });
    }
}
