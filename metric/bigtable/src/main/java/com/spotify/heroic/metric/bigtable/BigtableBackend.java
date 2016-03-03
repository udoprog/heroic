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

package com.spotify.heroic.metric.bigtable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.metric.bigtable.api.Column;
import com.spotify.heroic.metric.bigtable.api.DataClient;
import com.spotify.heroic.metric.bigtable.api.Mutations;
import com.spotify.heroic.metric.bigtable.api.MutationsBuilder;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.Row;
import com.spotify.heroic.metric.bigtable.api.RowFilter;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.metric.bigtable.api.TableAdminClient;
import com.spotify.heroic.metrics.Meter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.serializer.BytesSerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

@ToString(of = {"connection"})
@Slf4j
public class BigtableBackend extends AbstractMetricBackend implements LifeCycles {
    /* maxmimum number of cells supported for each batch mutation */
    public static final int MAX_BATCH_SIZE = 10000;

    public static final QueryTrace.Identifier FETCH_SEGMENT =
        QueryTrace.identifier(BigtableBackend.class, "fetch_segment");
    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(BigtableBackend.class, "fetch");

    public static final String METRICS = "metrics";
    public static final String POINTS = "points";
    public static final String EVENTS = "events";
    public static final long PERIOD = 0x100000000L;

    private final AsyncFramework async;
    private final SerializerFramework serializer;
    private final Serializer<RowKey> rowKeySerializer;
    private final Managed<BigtableConnection> connection;
    private final Groups groups;
    private final boolean configure;
    private final MetricBackendReporter reporter;
    private final ObjectMapper mapper;

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE =
        new TypeReference<Map<String, Object>>() {
        };

    private final Meter written = new Meter();

    @Inject
    public BigtableBackend(
        final AsyncFramework async, @Named("common") final SerializerFramework serializer,
        final Serializer<RowKey> rowKeySerializer, final Managed<BigtableConnection> connection,
        final Groups groups, @Named("configure") final boolean configure,
        MetricBackendReporter reporter, @Named("application/json") ObjectMapper mapper
    ) {
        super(async);
        this.async = async;
        this.serializer = serializer;
        this.rowKeySerializer = rowKeySerializer;
        this.connection = connection;
        this.groups = groups;
        this.configure = configure;
        this.reporter = reporter;
        this.mapper = mapper;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(new ManagedAction<BigtableConnection, Void>() {
            @Override
            public AsyncFuture<Void> action(final BigtableConnection c) throws Exception {
                return async.call(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        final TableAdminClient admin = c.adminClient();

                        configureMetricsTable(admin);

                        return null;
                    }

                    private void configureMetricsTable(final TableAdminClient admin)
                        throws IOException {
                        final Table metrics = admin.getTable(METRICS).orElseGet(() -> {
                            log.info("Creating missing table: " + METRICS);
                            return admin.createTable(METRICS);
                        });

                        metrics.getColumnFamily(POINTS).orElseGet(() -> {
                            log.info("Creating missing column family: " + POINTS);
                            return admin.createColumnFamily(metrics, POINTS);
                        });

                        metrics.getColumnFamily(EVENTS).orElseGet(() -> {
                            log.info("Creating missing column family: " + EVENTS);
                            return admin.createColumnFamily(metrics, EVENTS);
                        });
                    }
                });
            }
        });
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteResult> write(final WriteMetric w) {
        if (w.isEmpty()) {
            return async.resolved(WriteResult.EMPTY);
        }

        return connection.doto(c -> {
            final Series series = w.getSeries();
            final List<AsyncFuture<WriteResult>> results = new ArrayList<>();

            final DataClient client = c.client();

            final MetricCollection g = w.getData();

            switch (g.getType()) {
                case POINT:
                    results.add(writeBatch(POINTS, series, client, g.getDataAs(Point.class),
                        p -> serializeValue(p.getValue())));
                    break;
                case EVENT:
                    results.add(writeBatch(EVENTS, series, client, g.getDataAs(Event.class),
                        this::serializeEvent));
                    break;
                default:
                    break;
            }

            return async.collect(results, WriteResult.merger()).onDone(reporter.reportWrite());
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
        return connection.doto(new ManagedAction<BigtableConnection, WriteResult>() {
            @Override
            public AsyncFuture<WriteResult> action(final BigtableConnection c) throws Exception {
                final AtomicInteger count = new AtomicInteger(writes.size());
                final ResolvableFuture<WriteResult> future = async.future();

                for (WriteMetric w : writes) {
                    final Series series = w.getSeries();
                    final List<AsyncFuture<WriteResult>> results = new ArrayList<>();

                    final DataClient client = c.client();

                    final MetricCollection g = w.getData();

                    switch (g.getType()) {
                        case POINT:
                            results.add(writeBatch(POINTS, series, client, g.getDataAs(Point.class),
                                d -> serializeValue(d.getValue())));
                            break;
                        case EVENT:
                            results.add(writeBatch(EVENTS, series, client, g.getDataAs(Event.class),
                                BigtableBackend.this::serializeEvent));
                            break;
                        default:
                            break;
                    }

                    async
                        .collect(results, WriteResult.merger())
                        .onDone(new FutureDone<WriteResult>() {
                            final ConcurrentLinkedQueue<WriteResult> times =
                                new ConcurrentLinkedQueue<>();
                            final AtomicReference<Throwable> failed = new AtomicReference<>();

                            @Override
                            public void failed(Throwable cause) throws Exception {
                                check();
                                failed.compareAndSet(null, cause);
                            }

                            @Override
                            public void resolved(WriteResult result) throws Exception {
                                times.add(result);
                                check();
                            }

                            @Override
                            public void cancelled() throws Exception {
                                check();
                            }

                            private void check() {
                                if (count.decrementAndGet() == 0) {
                                    finish();
                                }
                            }

                            private void finish() {
                                final Throwable t = failed.get();

                                if (t != null) {
                                    future.fail(t);
                                    return;
                                }

                                final List<Long> times = new ArrayList<>();

                                for (final WriteResult r : this.times) {
                                    times.addAll(r.getTimes());
                                }

                                future.resolve(new WriteResult(times));
                            }
                        });
                }

                return future.onDone(reporter.reportWriteBatch());
            }
        });
    }

    @Override
    public AsyncFuture<FetchData> fetch(
        final MetricType type, final Series series, final DateRange range,
        final FetchQuotaWatcher watcher, final QueryOptions options
    ) {
        final List<PreparedQuery> prepared;

        try {
            prepared = ranges(series, range, rowKeySerializer);
        } catch (final IOException e) {
            return async.failed(e);
        }

        if (!watcher.mayReadData()) {
            throw new IllegalArgumentException("query violated data limit");
        }

        return connection.doto(new ManagedAction<BigtableConnection, FetchData>() {
            @Override
            public AsyncFuture<FetchData> action(final BigtableConnection c) throws Exception {
                switch (type) {
                    case POINT:
                        return fetchBatch(type, POINTS, series, prepared, c, options, (t, d) -> {
                            final double value = deserializeValue(d);
                            return new Point(t, value);
                        });
                    case EVENT:
                        return fetchBatch(type, EVENTS, series, prepared, c, options, (t, d) -> {
                            final Map<String, Object> payload;

                            try {
                                payload = mapper.readValue(d.toByteArray(), PAYLOAD_TYPE);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            return new Event(t, payload);
                        });
                    default:
                        return async.failed(new Exception("Unsupported type: " + type));
                }
            }
        });
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return ImmutableList.of();
    }

    @Override
    public Statistics getStatistics() {
        final long written = this.written.getCount();
        final double writeRate = this.written.getFiveMinuteRate();
        return Statistics.of("written", written, "writeRate", (long) writeRate);
    }

    private AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }

    private <T extends Metric> AsyncFuture<WriteResult> writeBatch(
        final String columnFamily, final Series series, final DataClient client,
        final List<T> batch, final Function<T, ByteString> serializer
    ) throws IOException {
        // common case for consumers
        if (batch.size() == 1) {
            return writeOne(columnFamily, series, client, batch.get(0), serializer).onFinished(
                written::mark);
        }

        final List<Pair<RowKey, Mutations>> saved = new ArrayList<>();
        final Map<RowKey, MutationsBuilder> building = new HashMap<>();

        for (final T d : batch) {
            final long timestamp = d.getTimestamp();
            final long base = base(timestamp);
            final long offset = offset(timestamp);

            final RowKey rowKey = new RowKey(series, base);

            MutationsBuilder builder = building.get(rowKey);

            final ByteString offsetBytes = serializeOffset(offset);
            final ByteString valueBytes = serializer.apply(d);

            if (builder == null) {
                builder = client.mutations();
                building.put(rowKey, builder);
            }

            builder.setCell(columnFamily, offsetBytes, valueBytes);

            if (builder.size() >= MAX_BATCH_SIZE) {
                saved.add(Pair.of(rowKey, builder.build()));
                building.put(rowKey, client.mutations());
            }
        }

        final ImmutableList.Builder<AsyncFuture<WriteResult>> writes = ImmutableList.builder();

        for (final Pair<RowKey, Mutations> e : saved) {
            final long start = System.nanoTime();
            final ByteString rowKeyBytes = serialize(e.getKey(), rowKeySerializer);
            writes.add(client
                .mutateRow(METRICS, rowKeyBytes, e.getValue())
                .directTransform(result -> WriteResult.of(System.nanoTime() - start)));
        }

        for (final Map.Entry<RowKey, MutationsBuilder> e : building.entrySet()) {
            final long start = System.nanoTime();
            final ByteString rowKeyBytes = serialize(e.getKey(), rowKeySerializer);
            writes.add(client
                .mutateRow(METRICS, rowKeyBytes, e.getValue().build())
                .directTransform(result -> WriteResult.of(System.nanoTime() - start)));
        }

        return async.collect(writes.build(), WriteResult.merger());
    }

    private <T extends Metric> AsyncFuture<WriteResult> writeOne(
        final String columnFamily, Series series, DataClient client, T p,
        Function<T, ByteString> serializer
    ) throws IOException {
        final long timestamp = p.getTimestamp();
        final long base = base(timestamp);
        final long offset = offset(timestamp);

        final RowKey rowKey = new RowKey(series, base);

        final MutationsBuilder builder = client.mutations();

        final ByteString offsetBytes = serializeOffset(offset);
        final ByteString valueBytes = serializer.apply(p);

        builder.setCell(columnFamily, offsetBytes, valueBytes);

        final long start = System.nanoTime();
        final ByteString rowKeyBytes = serialize(rowKey, rowKeySerializer);
        return client
            .mutateRow(METRICS, rowKeyBytes, builder.build())
            .directTransform(result -> WriteResult.of(System.nanoTime() - start));
    }

    private <T extends Metric> AsyncFuture<FetchData> fetchBatch(
        final MetricType type, final String columnFamily, final Series series,
        final List<PreparedQuery> prepared, final BigtableConnection c, final QueryOptions options,
        final BiFunction<Long, ByteString, T> deserializer
    ) {
        final List<AsyncFuture<FetchData>> queries = new ArrayList<>();

        final DataClient client = c.client();

        for (final PreparedQuery p : prepared) {
            final AsyncFuture<List<Row>> readRows = client.readRows(METRICS, ReadRowsRequest
                .builder()
                .rowKey(p.keyBlob)
                .filter(RowFilter
                    .newColumnRangeBuilder(columnFamily)
                    .startQualifierExclusive(p.startKey)
                    .endQualifierInclusive(p.endKey)
                    .build())
                .build());

            final Function<Column, T> transform = new Function<Column, T>() {
                @Override
                public T apply(Column cell) {
                    final long timestamp = p.base + deserializeOffset(cell.getQualifier());
                    return deserializer.apply(timestamp, cell.getValue());
                }
            };

            final Stopwatch w = Stopwatch.createStarted();

            queries.add(readRows.directTransform(result -> {
                final List<Iterable<T>> points = new ArrayList<>();

                for (final Row row : result) {
                    row.getFamily(columnFamily).ifPresent(f -> {
                        points.add(Iterables.transform(f.getColumns(), transform));
                    });
                }

                final QueryTrace trace =
                    new QueryTrace(FETCH_SEGMENT, w.elapsed(TimeUnit.NANOSECONDS));
                final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
                final List<T> data =
                    ImmutableList.copyOf(Iterables.mergeSorted(points, type.comparator()));
                final List<MetricCollection> groups =
                    ImmutableList.of(MetricCollection.build(type, data));
                return new FetchData(series, times, groups, trace);
            }));
        }

        return async
            .collect(queries, FetchData.collect(FETCH, series))
            .onDone(reporter.reportFetch());
    }

    <T> ByteString serialize(T rowKey, Serializer<T> serializer) throws IOException {
        try (final BytesSerialWriter writer = this.serializer.writeBytes()) {
            serializer.serialize(writer, rowKey);
            return ByteString.copyFrom(writer.toByteArray());
        }
    }

    static long base(long timestamp) {
        return timestamp - timestamp % PERIOD;
    }

    static long offset(long timestamp) {
        return timestamp % PERIOD;
    }

    List<PreparedQuery> ranges(
        final Series series, final DateRange range, final Serializer<RowKey> rowKeySerializer
    ) throws IOException {
        final List<PreparedQuery> bases = new ArrayList<>();

        final long start = base(range.getStart());
        final long end = base(range.getEnd());

        for (long base = start; base <= end; base += PERIOD) {
            final DateRange modified = range.modify(base, base + PERIOD);

            if (modified.isEmpty()) {
                continue;
            }

            final RowKey key = new RowKey(series, base);
            final ByteString keyBlob = serialize(key, rowKeySerializer);
            final ByteString startKey = serializeOffset(offset(modified.start()));
            final ByteString endKey = serializeOffset(offset(modified.end()));

            bases.add(new PreparedQuery(keyBlob, startKey, endKey, base));
        }

        return bases;
    }

    ByteString serializeValue(double value) {
        final ByteBuffer buffer =
            ByteBuffer.allocate(Double.BYTES).putLong(Double.doubleToLongBits(value));
        return ByteString.copyFrom(buffer.array());
    }

    ByteString serializeEvent(Event event) {
        try {
            return ByteString.copyFrom(mapper.writeValueAsBytes(event.getPayload()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static double deserializeValue(ByteString value) {
        return Double.longBitsToDouble(ByteBuffer.wrap(value.toByteArray()).getLong());
    }

    /**
     * Offset serialization is sensitive to byte ordering.
     * <p>
     * We require that for two timestamps a, and b, the following invariants hold true.
     * <p>
     * <pre>
     * offset(a) < offset(b)
     * serialized(offset(a)) < serialized(offset(b))
     * </pre>
     * <p>
     * Note: the serialized comparison is performed byte-by-byte, from lowest to highest address.
     *
     * @param offset Offset to serialize
     * @return A byte array, containing the serialized offset.
     */
    static ByteString serializeOffset(long offset) {
        if (offset >= PERIOD) {
            throw new IllegalArgumentException("can only serialize 32-bit wide values");
        }

        final byte[] bytes = new byte[4];
        bytes[0] = (byte) ((offset >> 24) & 0xff);
        bytes[1] = (byte) ((offset >> 16) & 0xff);
        bytes[2] = (byte) ((offset >> 8) & 0xff);
        bytes[3] = (byte) ((offset >> 0) & 0xff);
        return ByteString.copyFrom(bytes);
    }

    static long deserializeOffset(ByteString value) {
        final byte[] bytes = value.toByteArray();

        // @formatter:off
        return ((long) (bytes[0] & 0xff) << 24) +
               ((long) (bytes[1] & 0xff) << 16) +
               ((long) (bytes[2] & 0xff) << 8) +
               ((long) (bytes[3] & 0xff) << 0);
        // @formatter:on
    }

    @RequiredArgsConstructor
    private static final class PreparedQuery {
        private final ByteString keyBlob;
        private final ByteString startKey;
        private final ByteString endKey;
        private final long base;
    }
}
