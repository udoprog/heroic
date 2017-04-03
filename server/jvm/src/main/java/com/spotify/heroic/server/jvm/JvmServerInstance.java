package com.spotify.heroic.server.jvm;

import com.spotify.heroic.server.BodyWriter;
import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.OnceObserver;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerRequest;
import com.spotify.heroic.server.ServerResponse;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JvmServerInstance {
    private final AsyncFramework async;
    private final ServerInstance inner;

    /**
     * Perform a call without an entity.
     */
    public AsyncFuture<ServerResponse> call(final ServerRequest request) {
        return call(request, Optional.empty());
    }

    /**
     * Perform a call with an entity.
     */
    public AsyncFuture<ServerResponse> call(
        final ServerRequest request, final Optional<ByteBuffer> entity
    ) {
        final ServerRequest modifiedRequest = entity
            .map(e -> request.withHeaders(request
                .headers()
                .join(Headers.of("content-length", Integer.toString(e.remaining())))))
            .orElse(request);

        final ResolvableFuture<ServerResponse> future = async.future();

        final BodyWriter writer = inner.call(modifiedRequest, new OnceObserver<ServerResponse>() {
            @Override
            public void observe(final ServerResponse result) {
                future.resolve(result);
            }

            @Override
            public void fail(final Throwable error) {
                future.fail(error);
            }
        });

        entity.ifPresent(writer::write);
        writer.end();

        return future;
    }
}
