package com.spotify.heroic.metric.filesystem.wal;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.function.ThrowingConsumer;
import com.spotify.heroic.metric.filesystem.io.FilesFramework;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.scheduler.UniqueTaskHandle;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
class FileWal implements Wal {
    public static final String LOG_PREFIX = "log_";
    private static final long MAX_LOG_SIZE = 1024 * 1024 * 16;
    private static final BaseEncoding BASE16 = BaseEncoding.base16();
    private static final int MAX_PENDING_FLUSHES = 10;

    public static final EnumSet<StandardOpenOption> ID_PATH_WRITE_OPTIONS =
        EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.DSYNC);
    public static final EnumSet<StandardOpenOption> ID_PATH_READ_OPTIONS =
        EnumSet.of(StandardOpenOption.READ);
    public static final EnumSet<StandardOpenOption> READ_LOG_OPTIONS =
        EnumSet.of(StandardOpenOption.READ);

    private final AsyncFramework async;
    private final FilesFramework files;
    private final SerializerFramework serializer;
    private final ExecutorService flushThread;

    private final Path rootPath;
    private final Path idPath;
    private final Path temporaryIdPath;
    private final Serializer<Long> txIdSerializer;
    private final Serializer<TxIdFile> txIdFile;
    private final Serializer<Integer> valueSizeSerializer;
    private final Serializer<Integer> checksumSerializer;

    private final long flushDurationMillis;
    private final UniqueTaskHandle flushTask;

    /**
     * Current transaction ID.
     */
    private final AtomicLong currentId = new AtomicLong(Wal.DISABLED_TXID);
    private final AtomicLong walIdFileId = new AtomicLong(Wal.DISABLED_TXID);
    /**
     * Current open transaction file.
     */
    private volatile FileChannel file = null;

    private final ConcurrentSkipListSet<Long> pendingTransactions = new ConcurrentSkipListSet<>();
    private final LongAccumulator maxTxId = new LongAccumulator(Math::max, Wal.DISABLED_TXID);

    public FileWal(
        final AsyncFramework async, final FilesFramework files,
        final SerializerFramework serializer, final Scheduler scheduler, final Path rootPath,
        final Duration flushDuration, final long maxTransactionsPerFlush
    ) {
        this.async = async;
        this.files = files;
        this.serializer = serializer;
        this.flushThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("heroic-fs-flush-%d").build());

        this.rootPath = rootPath;
        this.idPath = rootPath.resolve("id");
        this.temporaryIdPath = rootPath.resolve(".id");
        this.txIdSerializer = serializer.fixedLong();
        this.txIdFile = new TxIdFile_Serializer(serializer);
        this.valueSizeSerializer = serializer.fixedInteger();
        this.checksumSerializer = serializer.fixedInteger();

        this.flushDurationMillis = flushDuration.toMilliseconds();
        this.flushTask = scheduler.unique("wal-flush");
    }

    @Override
    public AsyncFuture<Void> close() {
        return flushTask.stop().lazyTransform(ignore -> {
            return async.call(() -> {
                syncImmediate();

                if (file != null) {
                    file.close();
                    file = null;
                }

                return null;
            }, flushThread);
        });
    }

    @Override
    public <T> AsyncFuture<Void> write(
        final T value, final Serializer<T> valueSerializer,
        final ThrowingConsumer<Long> applyConsumer
    ) {
        return async.call(() -> {
            final long txId = this.currentId.incrementAndGet();

            final ByteArrayOutputStream byteArray = new ByteArrayOutputStream();

            try (final StreamSerialWriter buffer = serializer.writeStream(byteArray)) {
                txIdSerializer.serialize(buffer, txId);
                valueSerializer.serialize(buffer, value);
            }

            final byte[] bytes = byteArray.toByteArray();
            final int checksum = calculcateChecksum(bytes);

            flushImmediate(new WalBytesEntry(txId, bytes, checksum));
            applyConsumer.accept(txId);

            return null;
        }, flushThread);
    }

    @Override
    public void mark(final Set<Long> txIds) {
        for (final long txId : txIds) {
            maxTxId.accumulate(txId);
        }

        pendingTransactions.removeAll(txIds);
        scheduleSync();
    }

    private long getLatestCommittedId() {
        try {
            return pendingTransactions.first();
        } catch (NoSuchElementException e) {
            return maxTxId.get();
        }
    }

    private void removeLogsUntil(final long committedId) throws Exception {
        // if current id is the same as committed wal logs can be safely removed
        if (currentId.get() == committedId) {
            removeAllLogs();
            return;
        }

        final Iterator<WalPath> it = getLogPaths().iterator();

        if (!it.hasNext()) {
            return;
        }

        WalPath previous = it.next();

        while (it.hasNext()) {
            final WalPath current = it.next();

            if (current.getTxId() > committedId) {
                break;
            }

            log.trace("delete {}", previous);
            files.delete(previous.getPath());
            previous = current;
        }
    }

    private void removeAllLogs() throws Exception {
        for (final WalPath path : getLogPaths()) {
            log.trace("delete {}", path);
            files.delete(path.getPath());
        }
    }

    private int calculcateChecksum(final byte[] bytes) {
        final CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private void flushImmediate(final WalBytesEntry record) throws Exception {
        final FileChannel logFile = openLogFile(record);

        final byte[] bytes = record.getBytes();
        final int checksum = record.getChecksum();

        final SerialWriter buffer = serializer.writeByteChannel(logFile);

        valueSizeSerializer.serialize(buffer, bytes.length);
        buffer.write(bytes);
        checksumSerializer.serialize(buffer, checksum);

        pendingTransactions.add(record.getTxId());

        scheduleSync();
    }

    private void scheduleSync() {
        // sync periodically
        flushTask.schedule(flushDurationMillis, TimeUnit.MILLISECONDS, this::sync);
    }

    private AsyncFuture<Void> sync() {
        return async.call(() -> {
            syncImmediate();
            return null;
        }, flushThread);
    }

    private void syncImmediate() throws Exception {
        if (file != null) {
            file.force(true);
        }

        final long committedId = getLatestCommittedId();

        if (committedId > Wal.DISABLED_TXID && committedId != walIdFileId.get()) {
            writeWalId(committedId);
            removeLogsUntil(committedId);
        }
    }

    /**
     * Open, or return a channel to the appropriate log file.
     */
    private FileChannel openLogFile(final WalBytesEntry record) throws Exception {
        FileChannel file = this.file;

        if (file == null) {
            file = rotateLog(record.getTxId());
            this.file = file;
        }

        // rotate if record doesn't fit
        if (file.position() + record.getBytes().length > MAX_LOG_SIZE) {
            file = rotateLog(record.getTxId());
            this.file = file;
        }

        return file;
    }

    private FileChannel rotateLog(final long txId) throws Exception {
        if (file != null) {
            file.close();
        }

        final Path path = rootPath.resolve(String.format("%s%016x", LOG_PREFIX, txId));
        log.trace("writing {}", path);
        return files.newFileChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    @Override
    public <T> void recover(
        final Supplier<Recovery<T>> recoverySupplier, final Serializer<T> valueSerializer
    ) throws Exception {
        if (!files.isDirectory(rootPath)) {
            log.trace("creating directory: " + rootPath);
            files.createDirectory(rootPath);
        }

        recoverEntries(recoverySupplier, valueSerializer);
    }

    private <T> void recoverEntries(
        final Supplier<Recovery<T>> recoverySupplier, final Serializer<T> valueSerializer
    ) throws Exception {
        final TxIdFile walId = readWalId();

        final long committedId = walId.getCommittedId();
        long largestSeenTxId = committedId;

        final Iterator<WalPath> pathIterator = getLogPaths().iterator();
        final List<Pair<Path, String>> pathErrors = new ArrayList<>();

        while (pathIterator.hasNext()) {
            final WalPath walPath = pathIterator.next();

            log.info("recovering {}", walPath);

            final Path path = walPath.getPath();
            final long totalSize = files.size(path);
            int offset = -1;

            final Recovery<T> recovery = recoverySupplier.get();

            try (final SerialReader buffer = serializer.readByteChannel(
                files.newFileChannel(path, READ_LOG_OPTIONS))) {
                offset = -1;

                while (buffer.position() < totalSize) {
                    offset += 1;

                    final WalEntry<T> entry = deserializeEntry(buffer, totalSize, valueSerializer);

                    // skip records which we've already confirmed as committed.
                    if (entry.getTxId() <= committedId) {
                        continue;
                    }

                    if (entry.getTxId() < largestSeenTxId) {
                        throw new RuntimeException(
                            String.format("TxId out of order, found:%d, last:%d", entry.getTxId(),
                                largestSeenTxId));
                    }

                    largestSeenTxId = entry.getTxId();
                    recovery.consume(entry.getTxId(), entry.getValue());
                }
            } catch (final Exception e) {
                pathErrors.add(
                    Pair.of(path, String.format("entry #%d: %s", offset, e.getMessage())));
            }

            recovery.flush();
            log.trace("deleting {}", walPath);
            files.delete(walPath.getPath());
        }

        if (!pathErrors.isEmpty()) {
            for (final Pair<Path, String> error : pathErrors) {
                log.warn("bad log ({}): {}", error.getLeft(), error.getRight());
            }
        }

        maxTxId.accumulate(largestSeenTxId);
        writeWalId(largestSeenTxId);
        currentId.set(largestSeenTxId + 1);
    }

    /**
     * Deserialize a single entry.
     */
    private <T> WalEntry<T> deserializeEntry(
        final SerialReader buffer, final long totalSize, final Serializer<T> valueSerializer
    ) throws Exception {
        final long remaining = totalSize - buffer.position();

        if (remaining < 8) {
            throw new IllegalStateException(
                String.format("expected %d bytes, but had %d", 8, remaining));
        }

        final int size = valueSizeSerializer.deserialize(buffer);

        /* expect value size + checksum */
        final long expected = size + 4;

        if ((remaining - 4) < expected) {
            throw new IllegalStateException(
                String.format("expected %d bytes, but had %d", expected, remaining));
        }

        final byte[] bytes = new byte[size];
        buffer.read(bytes);

        final int expectedChecksum = checksumSerializer.deserialize(buffer);
        final int actualChecksum = calculcateChecksum(bytes);

        // check for corrupted record
        if (expectedChecksum != actualChecksum) {
            throw new IllegalStateException(
                String.format("checksum mismatch expected:%d != actual:%d", expectedChecksum,
                    actualChecksum));
        }

        final long txId;
        final T value;

        try (final SerialReader valueBuffer = serializer.readByteArray(bytes)) {
            txId = txIdSerializer.deserialize(valueBuffer);
            value = valueSerializer.deserialize(valueBuffer);
        }

        return new WalEntry<>(txId, value, expectedChecksum);
    }

    private SortedSet<WalPath> getLogPaths() throws IOException {
        final SortedSet<WalPath> paths = new TreeSet<>();

        for (final Path p : files.newDirectoryStream(rootPath)) {
            /* ignore non-log files */
            if (!p.getFileName().toString().startsWith(LOG_PREFIX)) {
                continue;
            }

            final long txId = decodeTxId(p.getFileName().toString());
            paths.add(new WalPath(p, txId));
        }

        return paths;
    }

    private void writeWalId(final long committedId) throws IOException {
        try (final SerialWriter buffer = serializer.writeByteChannel(
            files.newFileChannel(temporaryIdPath, ID_PATH_WRITE_OPTIONS))) {
            txIdFile.serialize(buffer, new TxIdFile(committedId, ~committedId));
        }

        files.move(temporaryIdPath, idPath, StandardCopyOption.ATOMIC_MOVE);
        walIdFileId.set(committedId);
    }

    private TxIdFile readWalId() throws IOException {
        final TxIdFile walId;

        try (final SerialReader buffer = serializer.readByteChannel(
            files.newFileChannel(idPath, ID_PATH_READ_OPTIONS))) {
            walId = txIdFile.deserialize(buffer);
        } catch (final NoSuchFileException e) {
            /* first transaction */
            return new TxIdFile(0L, ~0L);
        } catch (final Exception e) {
            throw new IOException(String.format("Failed to read ID from file (%s)", idPath), e);
        }

        final long expected = walId.calculateChecksum();
        final long actual = walId.getChecksum();

        if (expected != actual) {
            throw new IOException(
                String.format("ID checksum from file (%s) not valid, expected:%d != actual:%d",
                    idPath, expected, actual));
        }

        return walId;
    }

    static long decodeTxId(final String name) {
        final int end;

        /* remove file extension, if present */
        final int dotIndex = name.indexOf('.');

        if (dotIndex > 0) {
            end = dotIndex;
        } else {
            end = name.length();
        }

        final String nameId = name.substring(LOG_PREFIX.length(), end);
        return decodeNameBytes(BASE16.decode(nameId.toUpperCase()));
    }

    static long decodeNameBytes(final byte[] bytes) {
        long result = 0L;

        for (int i = 0; i < bytes.length; i++) {
            final long part = ((long) (bytes[i] & 0xff)) << ((bytes.length - 1 - i) * 8);
            result += part;
        }

        return result;
    }
}
