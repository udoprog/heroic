package com.spotify.heroic.metric.filesystem.wal;

import com.spotify.heroic.function.ThrowingConsumer;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.Serializer;
import java.util.Set;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class DisabledWal implements Wal {
    private final AsyncFramework async;

    @Override
    public AsyncFuture<Void> close() {
        return async.resolved();
    }

    @Override
    public <T> AsyncFuture<Void> write(
        final T value, final Serializer<T> valueSerializer, final ThrowingConsumer<Long> consumer
    ) throws Exception {
        consumer.accept(DISABLED_TXID);
        return async.resolved();
    }

    @Override
    public void mark(final Set<Long> txId) {
    }

    @Override
    public <T> void recover(
        final Supplier<Recovery<T>> recoverySupplier, final Serializer<T> valueSerializer
    ) throws Exception {
    }
}
