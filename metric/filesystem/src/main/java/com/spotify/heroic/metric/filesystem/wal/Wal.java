package com.spotify.heroic.metric.filesystem.wal;

import com.spotify.heroic.function.ThrowingConsumer;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.Serializer;
import java.util.Set;
import java.util.function.Supplier;

public interface Wal {
    /**
     * Special transaction id indicating that the WAL id disabled for the entry written.
     */
    long DISABLED_TXID = 0L;

    /**
     * Largest possible TxId, never expected to occur in the wild.
     */
    long LAST_TXID = Long.MAX_VALUE;

    /**
     * Writes the given entry to the log, and returns its transaction ID.
     */
    AsyncFuture<Void> close();

    /**
     * Write an entry to the transaction log.
     *
     * @param value value to write
     * @param valueSerializer serializer of value
     * @param <T> type of value
     * @return the transaction id of the entry
     */
    <T> AsyncFuture<Void> write(
        T value, Serializer<T> valueSerializer, ThrowingConsumer<Long> applyConsumer
    ) throws Exception;

    /**
     * Mark that the given transaction IDs have been committed to storage.
     */
    void mark(Set<Long> txIds);

    /**
     * Recover all transactions in the log.
     */
    <T> void recover(Supplier<Recovery<T>> recoverySupplier, Serializer<T> valueSerializer)
        throws Exception;

    interface Recovery<T> {
        void consume(long txId, T value) throws Exception;

        void flush() throws Exception;
    }
}
