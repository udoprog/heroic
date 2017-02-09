/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.filesystem;

import static com.spotify.heroic.metric.MetricType.EVENT;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.io.BaseEncoding;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.function.ThrowingBiConsumer;
import com.spotify.heroic.function.ThrowingBiFunction;
import com.spotify.heroic.function.ThrowingFunction;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.filesystem.io.FilesFramework;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentIterable;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import com.spotify.heroic.metric.filesystem.segmentio.SegmentEncoding;
import com.spotify.heroic.metric.filesystem.transaction.InMemoryState;
import com.spotify.heroic.metric.filesystem.transaction.SegmentInMemory;
import com.spotify.heroic.metric.filesystem.transaction.SegmentSnapshot;
import com.spotify.heroic.metric.filesystem.transaction.WriteEvents;
import com.spotify.heroic.metric.filesystem.transaction.WritePoints;
import com.spotify.heroic.metric.filesystem.transaction.WriteState;
import com.spotify.heroic.metric.filesystem.transaction.WriteTransaction;
import com.spotify.heroic.metric.filesystem.wal.Wal;
import com.spotify.heroic.metric.filesystem.wal.WalBuilder;
import com.spotify.heroic.metric.filesystem.wal.WalEntry;
import com.spotify.heroic.metric.filesystem.wal.WalReceiver;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.scheduler.UniqueTaskHandle;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@Singleton
@Slf4j
@Data
@ToString(of = {})
public class FilesystemBackend extends AbstractMetricBackend
    implements WalReceiver<Transaction>, LifeCycles {
    public static final int CREATE_DIRECTORY_CACHE_SIZE = 10000;

    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(FilesystemBackend.class, "fetch");

    public static final EnumSet<StandardOpenOption> CREATE_SEGMENT_OPTIONS =
        EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);

    private final AsyncFramework async;
    private final FilesFramework files;
    private final SerializerFramework serializer;
    private final Scheduler scheduler;
    private final Groups groups;
    private final Path storagePath;
    private final ExecutorService writers;

    private final long segmentWidthMillis;
    private final long segmentCacheSize;
    private final long flushIntervalMillis;
    private final int maxTransactionsPerFlush;
    private final Semaphore maxPendingTransactions;
    private final int transactionParallelismPerRequest;
    private final boolean useMemoryCache;
    private final Path lockPath;
    private final Serializer<Long> txIdSerializer;
    private final Wal<Transaction> wal;
    private final SegmentEncoding segmentEncoding;
    private final UniqueTaskHandle flushTask;

    /**
     * Points which are currently in memory, but will eventually get flushed to disk.
     */
    final ConcurrentSkipListMap<Long, Double> pointsInMemory = new ConcurrentSkipListMap<>();
    final ConcurrentSkipListMap<Long, Map<String, String>> eventsInMemory =
        new ConcurrentSkipListMap<>();

    final LoadingCache<SegmentKey, Optional<Segment<SegmentPoint>>> pointsSegments;
    final LoadingCache<SegmentKey, Optional<Segment<SegmentEvent>>> eventsSegments;
    final LoadingCache<Path, Boolean> parentDirectoryCheck;

    /**
     * Transactions waiting to be applied by a flush.
     */
    final ConcurrentLinkedQueue<SegmentIterable<WalEntry<Transaction>>> transactions;

    /**
     * Pending in-memory transactions that should soon be flushed
     */
    private final AtomicReference<FileLock> lock = new AtomicReference<>();

    /**
     * Flush task and lock.
     */
    private final Object flushLock = new Object();
    private volatile UUID flushNextId = null;

    final StreamCollector<Void, Void> ignoreCollector = new StreamCollector<Void, Void>() {
        @Override
        public void resolved(final Void result) throws Exception {
        }

        @Override
        public void failed(final Throwable cause) throws Exception {
        }

        @Override
        public void cancelled() throws Exception {
        }

        @Override
        public Void end(final int resolved, final int failed, final int cancelled)
            throws Exception {
            if (failed + cancelled > 0) {
                throw new RuntimeException("operation failed");
            }

            return null;
        }
    };

    @Inject
    public FilesystemBackend(
        final AsyncFramework async, final FilesFramework files,
        final SerializerFramework serializer, final Groups groups,
        @Named("storagePath") final Path storagePath,
        @Named("segmentWidth") final Duration segmentWidth,
        @Named("segmentCacheSize") final long segmentCacheSize,
        @Named("flushInterval") final Duration flushInterval,
        @Named("maxTransactionsPerFlush") final int maxTransactionsPerFlush,
        @Named("maxPendingTransactions") final int maxPendingTransactions,
        @Named("transactionParallelismPerRequest") final int transactionParallelismPerRequest,
        @Named("useMemoryCache") final boolean useMemoryCache,
        @Named("writers") final ExecutorService writers, final Scheduler scheduler,
        final Compression compression, final WalBuilder<Transaction> walBuilder
    ) {
        super(async);
        this.async = async;
        this.files = files;
        this.serializer = serializer;
        this.scheduler = scheduler;
        this.groups = groups;
        this.storagePath = storagePath;
        this.writers = writers;

        this.segmentWidthMillis = segmentWidth.toMilliseconds();
        this.segmentCacheSize = segmentCacheSize;
        this.flushIntervalMillis = flushInterval.toMilliseconds();
        this.maxTransactionsPerFlush = maxTransactionsPerFlush;
        this.maxPendingTransactions = new Semaphore(maxPendingTransactions);
        this.transactionParallelismPerRequest = transactionParallelismPerRequest;
        this.useMemoryCache = useMemoryCache;
        this.lockPath = storagePath.resolve("lock");
        this.txIdSerializer = serializer.fixedLong();

        this.wal = walBuilder.build(this);

        this.segmentEncoding = compression.newSegmentIO(serializer);

        this.pointsSegments = buildSegmentCache(MetricType.POINT, SegmentEncoding::readPoints);
        this.eventsSegments = buildSegmentCache(EVENT, SegmentEncoding::readEvents);

        this.parentDirectoryCheck = CacheBuilder
            .newBuilder()
            .maximumSize(CREATE_DIRECTORY_CACHE_SIZE)
            .build(new CacheLoader<Path, Boolean>() {
                @Override
                public Boolean load(final Path path) throws Exception {
                    return ensureParentDirectories(path);
                }
            });

        this.transactions = new ConcurrentLinkedQueue<>();

        this.flushTask = scheduler.unique(FilesystemBackend.class + "#flush");
    }

    @Override
    public Statistics getStatistics() {
        final long memoryCacheSize = pointsInMemory.size() + eventsInMemory.size();
        return Statistics.of("memory-cache-size", memoryCacheSize, "transactions-pending",
            transactions.size());
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteMetric> write(WriteMetric.Request request) {
        final Stopwatch watch = Stopwatch.createStarted();

        final HashCode id = request.getSeries().getHashCode();

        final List<Callable<AsyncFuture<Void>>> futures = new ArrayList<>();

        final List<WriteTransaction> pending = toTransactions(id, request.getData());

        for (final WriteTransaction transaction : pending) {
            futures.add(() -> commit(transaction));
        }

        return async
            .eventuallyCollect(futures, ignoreCollector, transactionParallelismPerRequest)
            .directTransform(ignore -> WriteMetric.of(watch.elapsed(TimeUnit.MICROSECONDS)));
    }

    private List<WriteTransaction> toTransactions(final HashCode id, final MetricCollection data) {
        switch (data.getType()) {
            case POINT:
                return metricsToTransactions(id, data.getDataAs(Point.class), WritePoints::new);
            case EVENT:
                return metricsToTransactions(id, data.getDataAs(Event.class), WriteEvents::new);
            default:
                throw new RuntimeException("Unsupported type: " + data.getType());
        }
    }

    private <T extends Metric, Tx extends WriteTransaction> List<Tx> metricsToTransactions(
        final HashCode id, final List<T> input,
        final BiFunction<SegmentKey, List<T>, Tx> toTransaction
    ) {
        final Map<SegmentKey, List<T>> result = new HashMap<>();

        for (final T m : input) {
            final long ts = m.getTimestamp();
            final long base = ts - ts % segmentWidthMillis;
            final SegmentKey key = new SegmentKey(id, base);

            List<T> data = result.get(key);

            if (data == null) {
                data = new ArrayList<>();
                result.put(key, data);
            }

            data.add(m);
        }

        final List<Tx> transactions = new ArrayList<>();

        for (final Map.Entry<SegmentKey, List<T>> e : result.entrySet()) {
            transactions.add(toTransaction.apply(e.getKey(), e.getValue()));
        }

        return transactions;
    }

    @Override
    public AsyncFuture<FetchData> fetch(
        final FetchData.Request request, final FetchQuotaWatcher watcher
    ) {
        final Tracing tracing = request.getOptions().getTracing();

        final QueryTrace.NamedWatch watch = tracing.watch(FETCH);

        return async.call(() -> {
            final MetricCollection group;

            switch (request.getType()) {
                case POINT:
                    group = doFetchPoints(request, watcher);
                    break;
                case EVENT:
                    group = doFetchEvent(request, watcher);
                    break;
                default:
                    return FetchData.error(QueryTrace.PASSIVE,
                        QueryError.fromMessage("Unsupported type: " + request.getType()));
            }

            return FetchData.of(watch.end(), ImmutableList.of(), ImmutableList.of(group));
        });
    }

    private MetricCollection doFetchEvent(
        final FetchData.Request request, final FetchQuotaWatcher watcher
    ) throws Exception {
        return MetricCollection.events(
            doFetch(request, watcher, eventsSegments, eventsInMemory, this::segmentEventToEvent,
                SegmentEvent::new, SegmentEvent::getTimestamp));
    }

    private MetricCollection doFetchPoints(
        final FetchData.Request request, final FetchQuotaWatcher watcher
    ) throws Exception {
        return MetricCollection.points(
            doFetch(request, watcher, pointsSegments, pointsInMemory, this::segmentPointToPoint,
                SegmentPoint::new, SegmentPoint::getTimestamp));
    }

    private Event segmentEventToEvent(final SegmentEvent s) {
        return new Event(s.getTimestamp(), s.getPayload());
    }

    private Point segmentPointToPoint(final SegmentPoint s) {
        return new Point(s.getTimestamp(), s.getValue());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return Collections.emptyList();
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        return async.resolved();
    }

    @Override
    public void register(final LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    private <T extends Comparable<T>> LoadingCache<SegmentKey, Optional<Segment<T>>>
    buildSegmentCache(
        final MetricType type,
        final ThrowingBiFunction<SegmentEncoding, ByteBuffer, SegmentIterator<T>> reader
    ) {
        final CacheLoader<SegmentKey, Optional<Segment<T>>> loader =
            new CacheLoader<SegmentKey, Optional<Segment<T>>>() {
                public Optional<Segment<T>> load(final SegmentKey key) throws Exception {
                    return openSegmentRead(type, key, reader);
                }
            };

        return CacheBuilder.newBuilder().maximumSize(segmentCacheSize).build(loader);
    }

    private AsyncFuture<Void> flush() {
        log.trace("flushing");

        return flushImmediate().onFailed(throwable -> {
            log.error("flush failed", throwable);
        });
    }

    private void scheduleFlush() {
        if (flushIntervalMillis <= 0L) {
            flushTask.scheduleNow(this::flush);
        } else {
            flushTask.schedule(flushIntervalMillis, TimeUnit.MILLISECONDS, this::flush);
        }
    }

    private AsyncFuture<Void> flushImmediate() {
        return async.call(() -> {
            while (true) {
                final SegmentIterable<WalEntry<Transaction>> iterable = transactions.poll();

                if (iterable == null) {
                    break;
                }

                try (final SegmentIterator<WalEntry<Transaction>> iterator = iterable.iterator()) {
                    flushSegmentIterator(iterator);
                }
            }

            return null;
        }, writers);
    }

    private void flushSegmentIterator(final SegmentIterator<WalEntry<Transaction>> iterator)
        throws Exception {
        while (iterator.hasNext()) {
            int flushed = 0;

            final Set<Long> txIds = new HashSet<>();

            final WriteState state = new WriteState(new SegmentSnapshot<>(pointsSegments),
                new SegmentSnapshot<>(eventsSegments));

            final List<Transaction> undoInMemory = new ArrayList<>();

            while (iterator.hasNext()) {
                final WalEntry<Transaction> entry = iterator.next();

                txIds.add(entry.getTxId());
                state.lastTxId = entry.getTxId();

                final Transaction transaction = entry.getValue();
                transaction.write(state, entry.getTxId());

                if (useMemoryCache) {
                    undoInMemory.add(transaction);
                }

                flushed++;

                if (flushed >= maxTransactionsPerFlush) {
                    break;
                }
            }

            flushWriteState(state);

            final InMemoryState undoState = new InMemoryState(new SegmentInMemory<>(pointsInMemory),
                new SegmentInMemory<>(eventsInMemory));

            if (useMemoryCache) {
                for (final Transaction undo : undoInMemory) {
                    undo.undoInMemory(undoState);
                }
            }

            wal.mark(txIds);
        }
    }

    private void flushWriteState(final WriteState state) throws Exception {
        if (state.points.changed) {
            flushSegments(MetricType.POINT, state.lastTxId, pointsSegments, state.points.snapshot,
                input -> segmentEntryIterator(input, Wal.LAST_TXID, SegmentPoint::new),
                segmentEncoding::writePoints);
        }

        if (state.events.changed) {
            flushSegments(EVENT, state.lastTxId, eventsSegments, state.events.snapshot,
                input -> segmentEntryIterator(input, Wal.LAST_TXID, SegmentEvent::new),
                segmentEncoding::writeEvents);
        }
    }

    private <T extends Comparable<T>, V> SegmentIterator<T> segmentEntryIterator(
        final NavigableMap<Long, V> input, final long txId,
        final BiFunction<Long, V, T> toSegmentEntry
    ) {
        final Iterator<Map.Entry<Long, V>> entries = input.entrySet().iterator();

        final long slot = slotForTxId(txId);

        return new SegmentIterator<T>() {
            @Override
            public boolean hasNext() throws IOException {
                return entries.hasNext();
            }

            @Override
            public T next() throws IOException {
                final Map.Entry<Long, V> e = entries.next();
                return toSegmentEntry.apply(e.getKey(), e.getValue());
            }

            @Override
            public long slot() {
                return slot;
            }
        };
    }

    /**
     * Calculate the slot for a given TxId.
     * <p>
     * operations with a higher txId should have a higher priority, which is determined by
     * the smallest comparable value.
     *
     * @see com.spotify.heroic.metric.filesystem.io.SegmentIterator#slot()
     */
    private long slotForTxId(final long txId) {
        return Wal.LAST_TXID - txId;
    }

    private <T extends Comparable<T>, V> void flushSegments(
        final MetricType type, final long txId,
        final LoadingCache<SegmentKey, Optional<Segment<T>>> segmentsCache,
        final Map<SegmentKey, NavigableMap<Long, V>> toWrite,
        final ThrowingFunction<NavigableMap<Long, V>, SegmentIterator<T>> writeConverter,
        final ThrowingBiConsumer<FileChannel, SegmentIterable<T>> commit
    ) throws Exception {
        for (final Map.Entry<SegmentKey, NavigableMap<Long, V>> e : toWrite.entrySet()) {
            final SegmentKey key = e.getKey();
            final NavigableMap<Long, V> value = e.getValue();

            final Path temporary = temporarySegmentPath(type, key);
            final Path target = segmentPath(type, key);

            ensureParentDirectoriesCached(target.getParent());

            final Stopwatch w = Stopwatch.createStarted();

            final Optional<Segment<T>> segment = segmentsCache.get(key);

            try (final FileChannel channel = files.newFileChannel(temporary,
                CREATE_SEGMENT_OPTIONS)) {
                segmentEncoding.writeHeader(channel, new SegmentHeader(txId));

                commit.accept(channel, () -> {
                    final List<SegmentIterator<T>> iterators = new ArrayList<>();

                    // add this first so it takes precedence
                    iterators.add(writeConverter.apply(value));

                    // if existing segment is present, merge that data in
                    if (segment.isPresent()) {
                        iterators.add(segment.get().open());
                    }

                    return SegmentIterator.mergeUnique(iterators);
                });
            }

            files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);

            log.trace("wrote {} (in {} us)", target, w.elapsed(TimeUnit.MICROSECONDS));
            log.trace("invalidating {}", key);

            segmentsCache.invalidate(key);
        }
    }

    @Override
    public void receive(
        final SegmentIterable<WalEntry<Transaction>> transactions
    ) throws Exception {
        this.transactions.add(transactions);
    }

    private AsyncFuture<Void> commit(final Transaction transaction) throws Exception {
        if (useMemoryCache) {
            final InMemoryState state = new InMemoryState(new SegmentInMemory<>(pointsInMemory),
                new SegmentInMemory<>(eventsInMemory));

            transaction.writeInMemory(state);
        }

        return wal.write(transaction).onResolved(ignore -> scheduleFlush());
    }

    private <S extends Comparable<S>, T, U> List<U> doFetch(
        final FetchData.Request request, final FetchQuotaWatcher watcher,
        final LoadingCache<SegmentKey, Optional<Segment<S>>> segmentsCache,
        final ConcurrentSkipListMap<Long, T> inMemory, final Function<S, U> converter,
        final BiFunction<Long, T, S> toSegmentEntry, final Function<S, Long> segmentTimestamp
    ) throws Exception {
        final HashCode id = request.getSeries().getHashCode();
        final List<SegmentQuery> segmentQueries = buildSegments(id, request.getRange());
        final List<U> result = new ArrayList<>();

        for (final SegmentQuery q : segmentQueries) {
            final Optional<Segment<S>> segment = segmentsCache.get(q.getKey());

            final long start = q.getRange().getStart();
            final long end = q.getRange().getEnd();

            final List<SegmentIterator<S>> iterators = new ArrayList<>();

            // get file-based segments, if present
            if (segment.isPresent()) {
                iterators.add(segment.get().open().filtering(s -> {
                    final long timestamp = segmentTimestamp.apply(s);
                    return start < timestamp && timestamp <= end;
                }));
            }

            // use in-memory cache, if configured to do so
            if (useMemoryCache) {
                iterators.add(
                    segmentEntryIterator(inMemory.subMap(start, false, end, true), Wal.LAST_TXID,
                        toSegmentEntry));
            }

            try (final SegmentIterator<S> iterator = SegmentIterator.mergeUnique(iterators)) {
                while (iterator.hasNext()) {
                    result.add(converter.apply(iterator.next()));
                }
            }
        }

        watcher.readData(result.size());
        return result;
    }

    private List<SegmentQuery> buildSegments(final HashCode id, final DateRange range) {
        final ImmutableList.Builder<SegmentQuery> segments = ImmutableList.builder();

        final long start = range.getStart() - range.getStart() % segmentWidthMillis;
        final long end = range.getEnd() - range.getEnd() % segmentWidthMillis;

        for (long base = start; base <= end; base += segmentWidthMillis) {
            final DateRange modified = range.modify(base - 1, base + segmentWidthMillis);
            segments.add(new SegmentQuery(new SegmentKey(id, base), modified));
        }

        return segments.build();
    }

    private AsyncFuture<Void> start() {
        return async.call(() -> {
            if (!files.isDirectory(storagePath)) {
                log.trace("creating directory: {}", storagePath);
                files.createDirectory(storagePath);
            }

            acquireLock();

            final Supplier<Wal.Recovery<Transaction>> recovery =
                () -> new Wal.Recovery<Transaction>() {
                    final WriteState state = new WriteState(new SegmentSnapshot<>(pointsSegments),
                        new SegmentSnapshot<>(eventsSegments));

                    @Override
                    public void consume(final long txId, final Transaction transaction)
                        throws Exception {
                        state.lastTxId = txId;
                        transaction.write(state, txId);
                    }

                    @Override
                    public void flush() throws Exception {
                        flushWriteState(state);
                    }
                };

            wal.recover(recovery);
            return null;
        });
    }

    private AsyncFuture<Void> stop() {
        AsyncFuture<Void> all = flushTask.stop();

        all = all.lazyTransform(ignore -> flush());
        all = all.lazyTransform(ignore -> wal.close());

        return all.onFinished(this::releaseLock);
    }

    private void releaseLock() throws Exception {
        final FileLock lock = this.lock.getAndSet(null);

        if (lock != null) {
            log.trace("releasing: " + lockPath);
            lock.release();
            files.delete(lockPath);
        }
    }

    private void acquireLock() throws Exception {
        final FileChannel channel = new RandomAccessFile(lockPath.toFile(), "rw").getChannel();

        log.trace("acquiring: " + lockPath);
        final FileLock lock = channel.tryLock();

        if (lock == null) {
            throw new IllegalStateException("Lock is busy: " + lockPath);
        }

        this.lock.set(lock);
    }

    final BaseEncoding base16 = BaseEncoding.base16();

    private Path temporarySegmentPath(final MetricType type, final SegmentKey key) {
        return segmentPath(type, key, name -> "." + name);
    }

    private Path segmentPath(final MetricType type, final SegmentKey key) {
        return segmentPath(type, key, Function.identity());
    }

    private Path segmentPath(
        final MetricType type, final SegmentKey key, final Function<String, String> filterLast
    ) {
        final String id = base16.encode(key.getId().asBytes()).toLowerCase();

        final String base = id + "_" + String.format("%08x", segmentWidthMillis) + "_" +
            String.format("%016x", key.getBase());

        final String name = segmentEncoding.applyFileName(filterLast.apply(base));

        return storagePath.resolve(type.identifier()).resolve(id.substring(0, 2)).resolve(name);
    }

    private <T extends Comparable<T>> Optional<Segment<T>> openSegmentRead(
        final MetricType type, final SegmentKey key,
        final ThrowingBiFunction<SegmentEncoding, ByteBuffer, SegmentIterator<T>> reader
    ) throws Exception {
        final Path segmentPath = segmentPath(type, key);

        final SegmentHeader header;
        final long headerSize;

        try (final FileChannel channel = files.newFileChannel(segmentPath,
            EnumSet.of(StandardOpenOption.READ))) {
            header = segmentEncoding.readHeader(channel);
            headerSize = channel.position();
        } catch (final NoSuchFileException e) {
            return Optional.empty();
        }

        final long slot = slotForTxId(header.getTxId());

        return Optional.of(
            new Segment<>(segmentEncoding, segmentPath, header, headerSize, (encoding, buffer) -> {
                return reader.apply(encoding, buffer).withSlot(slot);
            }));
    }

    private void ensureParentDirectoriesCached(final Path path) throws Exception {
        parentDirectoryCheck.get(path);
    }

    private boolean ensureParentDirectories(final Path source) throws Exception {
        log.trace("ensuring directory exists: {}", source);

        // ensure target directories exist
        Path current = source;

        final List<Path> create = new ArrayList<>();

        while (!files.isDirectory(current)) {
            create.add(current);
            current = current.getParent();
        }

        boolean created = false;

        for (final Path path : Lists.reverse(create)) {
            log.trace("creating directory {}", path);

            try {
                files.createDirectory(path);
                created = true;
            } catch (final FileAlreadyExistsException e) {
                break;
            }
        }

        return created;
    }

    @Data
    private static class SegmentQuery {
        private final SegmentKey key;
        private final DateRange range;
    }
}
