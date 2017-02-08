package com.spotify.heroic.metric.filesystem.wal;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.filesystem.io.SegmentIterable;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Set;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class DisabledWal<T> implements Wal<T> {
    private final AsyncFramework async;
    private final WalReceiver<T> receiver;

    @Override
    public AsyncFuture<Void> close() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<Void> write(final T value) throws Exception {
        receiver.receive(SegmentIterable.fromIterable(
            ImmutableList.of(new WalEntry<>(DISABLED_TXID, value, 0))));
        return async.resolved();
    }

    @Override
    public void mark(final Set<Long> txId) {
    }

    @Override
    public void recover(final Supplier<Recovery<T>> recoverySupplier) throws Exception {
    }
}
