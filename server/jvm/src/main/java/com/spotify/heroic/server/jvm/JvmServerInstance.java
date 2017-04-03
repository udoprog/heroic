package com.spotify.heroic.server.jvm;

import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.Observer;
import com.spotify.heroic.server.OnceObservable;
import com.spotify.heroic.server.OnceObserver;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerResponse;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JvmServerInstance {
    private final AsyncFramework async;
    private final ServerInstance inner;

    public AsyncFuture<ServerResponse> call(
        final String method, final String path, final Headers headers, final ByteBuffer body
    ) {
        final ResolvableFuture<ServerResponse> future = async.future();

        final Consumer<OnceObservable<ServerResponse>> response = responseOnceObservable -> {
            responseOnceObservable.observe(new OnceObserver<ServerResponse>() {
                @Override
                public void observe(final ServerResponse result) {
                    future.resolve(result);
                }

                @Override
                public void fail(final Throwable error) {
                    future.fail(error);
                }
            });
        };

        inner
            .call(method, path, headers, response)
            .observe(new OnceObserver<Observer<ByteBuffer>>() {
                @Override
                public void observe(final Observer<ByteBuffer> result) {
                    result.observe(body);
                    result.end();
                }

                @Override
                public void fail(final Throwable error) {
                    future.fail(error);
                }
            });

        return future;
    }
}
