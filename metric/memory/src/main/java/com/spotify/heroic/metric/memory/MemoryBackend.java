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

package com.spotify.heroic.metric.memory;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricKey;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.ToString;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString(exclude = {"storage", "async", "createLock"})
public class MemoryBackend extends AbstractMetricBackend {
    private static final QueryTrace.Identifier WRITE =
        QueryTrace.identifier(MemoryBackend.class, "write");

    public static final String MEMORY_KEYS = "memory-keys";

    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(MemoryBackend.class, "fetch");

    static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    static final <T> Comparator<Optional<T>> optionalComparator(final Comparator<T> inner) {
        return (a, b) -> {
            if (a.isPresent() && b.isPresent()) {
                return inner.compare(a.get(), b.get());
            }

            if (a.isPresent()) {
                return 1;
            }

            return -1;
        };
    }

    static final <T, U> Comparator<SortedMap<T, U>> sortedMapComparator(
        final Comparator<T> key, final Comparator<U> value
    ) {
        return (a, b) -> {
            final Iterator<Map.Entry<T, U>> aIt = a.entrySet().iterator();
            final Iterator<Map.Entry<T, U>> bIt = b.entrySet().iterator();

            while (aIt.hasNext() && bIt.hasNext()) {
                final Map.Entry<T, U> aEntry = aIt.next();
                final Map.Entry<T, U> bEntry = bIt.next();

                final int k = key.compare(aEntry.getKey(), bEntry.getKey());

                if (k != 0) {
                    return k;
                }

                final int v = value.compare(aEntry.getValue(), bEntry.getValue());

                if (v != 0) {
                    return k;
                }
            }

            if (aIt.hasNext()) {
                return 1;
            }

            if (bIt.hasNext()) {
                return -1;
            }

            return 0;
        };
    }

    static final Comparator<MetricKey> METRIC_KEY_COMPARATOR = (a, b) -> {
        return ComparisonChain
            .start()
            .compare(a.getKey(), b.getKey(), optionalComparator(String::compareTo))
            .compare(a.getTags(), b.getTags(),
                sortedMapComparator(String::compareTo, String::compareTo))
            .result();
    };

    static final Comparator<MemoryKey> MEMORY_KEY_COMPARATOR = (a, b) -> {
        return ComparisonChain
            .start()
            .compare(a.getType(), b.getType())
            .compare(a.getKey(), b.getKey(), METRIC_KEY_COMPARATOR)
            .result();
    };

    private final Object createLock = new Object();

    private final AsyncFramework async;
    private final Groups groups;
    private final Map<MemoryKey, NavigableMap<Long, Metric>> storage;

    @Inject
    public MemoryBackend(
        final AsyncFramework async, final Groups groups,
        @Named("storage") final Map<MemoryKey, NavigableMap<Long, Metric>> storage,
        LifeCycleRegistry registry
    ) {
        super(async);
        this.async = async;
        this.groups = groups;
        this.storage = storage;
    }

    @Override
    public Statistics getStatistics() {
        return Statistics.of(MEMORY_KEYS, storage.size());
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
        final QueryTrace.NamedWatch watch = request.getTracing().watch(WRITE);
        writeOne(request);
        return async.resolved(WriteMetric.of(watch.end()));
    }

    @Override
    public AsyncFuture<FetchData> fetch(FetchData.Request request, FetchQuotaWatcher watcher) {
        final QueryTrace.NamedWatch w = request.getTracing().watch(FETCH);
        final List<MetricCollection> groups =
            doFetch(request.getKey(), request.getType(), request.getRange(), watcher);
        return async.resolved(FetchData.of(w.end(), ImmutableList.of(), groups));
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        storage.remove(new MemoryKey(key.toMetricKey(), key.getType()));
        return async.resolved();
    }

    private void writeOne(final WriteMetric.Request request) {
        final MetricCollection g = request.getData();

        final NavigableMap<Long, Metric> tree =
            getOrCreate(request.getKey(), request.getData().getType());

        synchronized (tree) {
            for (final Metric d : g.getData()) {
                tree.put(d.getTimestamp(), d);
            }
        }
    }

    private List<MetricCollection> doFetch(
        final MetricKey key, final MetricType type, final DateRange range,
        final FetchQuotaWatcher watcher
    ) {
        final NavigableMap<Long, Metric> tree = storage.get(new MemoryKey(key, type));

        if (tree == null) {
            return ImmutableList.of(MetricCollection.build(type, ImmutableList.of()));
        }

        synchronized (tree) {
            final Iterable<Metric> metrics = tree.subMap(range.getStart(), range.getEnd()).values();
            final List<Metric> data = ImmutableList.copyOf(metrics);
            watcher.readData(data.size());
            return ImmutableList.of(MetricCollection.build(type, data));
        }
    }

    /**
     * Get or create a new navigable map to store time data.
     *
     * @param key The key to create the map under.
     * @return An existing, or a newly created navigable map for the given key.
     */
    private NavigableMap<Long, Metric> getOrCreate(final MetricKey key, final MetricType type) {
        final MemoryKey k = new MemoryKey(key, type);
        final NavigableMap<Long, Metric> tree = storage.get(k);

        if (tree != null) {
            return tree;
        }

        synchronized (createLock) {
            final NavigableMap<Long, Metric> checked = storage.get(k);

            if (checked != null) {
                return checked;
            }

            final NavigableMap<Long, Metric> created = new TreeMap<>();
            storage.put(new MemoryKey(key, type), created);
            return created;
        }
    }

    @Data
    static final class MemoryKey {
        private final MetricKey key;
        private final MetricType type;
    }
}
