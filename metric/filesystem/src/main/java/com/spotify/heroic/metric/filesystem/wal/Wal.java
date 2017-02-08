package com.spotify.heroic.metric.filesystem.wal;

import eu.toolchain.async.AsyncFuture;
import java.util.Set;
import java.util.function.Supplier;

public interface Wal<T> {
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
     * @return the transaction id of the entry
     */
    AsyncFuture<Void> write(T value) throws Exception;

    /**
     * Mark that the given transaction IDs have been committed to storage.
     */
    void mark(Set<Long> txIds);

    /**
     * Recover all transactions in the log.
     */
    void recover(Supplier<Recovery<T>> recoverySupplier) throws Exception;

    interface Recovery<T> {
        void consume(long txId, T value) throws Exception;

        void flush() throws Exception;
    }
}
