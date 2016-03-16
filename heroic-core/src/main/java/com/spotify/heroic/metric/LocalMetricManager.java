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

package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.spotify.heroic.FullQuery;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.grammar.DefaultScope;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.ResolvableFuture;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@ToString(of = {})
public class LocalMetricManager implements MetricManager {
    private static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(LocalMetricManager.class, "query");
    private static final QueryTrace.Identifier ANALYZE =
        QueryTrace.identifier(LocalMetricManager.class, "analyze");
    private static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(LocalMetricManager.class, "fetch");
    private static final QueryTrace.Identifier KEYS =
        QueryTrace.identifier(LocalMetricManager.class, "keys");

    public static final FetchQuotaWatcher NO_QUOTA_WATCHER = new FetchQuotaWatcher() {
        @Override
        public boolean readData(long n) {
            return true;
        }

        @Override
        public boolean mayReadData() {
            return true;
        }

        @Override
        public int getReadDataQuota() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isQuotaViolated() {
            return false;
        }
    };

    private final int groupLimit;
    private final int seriesLimit;
    private final long aggregationLimit;
    private final long dataLimit;
    private final int fetchParallelism;

    private final AsyncFramework async;
    private final BackendGroups<MetricBackend> backends;
    private final MetadataManager metadata;
    private final MetricBackendGroupReporter reporter;

    /**
     * @param groupLimit The maximum amount of groups this manager will allow to be generated.
     * @param seriesLimit The maximum amount of series in total an entire query may use.
     * @param aggregationLimit The maximum number of (estimated) data points a single aggregation
     * may produce.
     * @param dataLimit The maximum number of samples a single query is allowed to fetch.
     * @param fetchParallelism How many fetches that are allowed to be performed in parallel.
     */
    public LocalMetricManager(
        final int groupLimit, final int seriesLimit, final long aggregationLimit,
        final long dataLimit, final int fetchParallelism, final AsyncFramework async,
        final BackendGroups<MetricBackend> backends, final MetadataManager metadata,
        final MetricBackendGroupReporter reporter
    ) {
        this.groupLimit = groupLimit;
        this.seriesLimit = seriesLimit;
        this.aggregationLimit = aggregationLimit;
        this.dataLimit = dataLimit;
        this.fetchParallelism = fetchParallelism;
        this.async = async;
        this.backends = backends;
        this.metadata = metadata;
        this.reporter = reporter;
    }

    @Override
    public List<MetricBackend> allMembers() {
        return backends.allMembers();
    }

    @Override
    public List<MetricBackend> use(String group) {
        return backends.use(group).getMembers();
    }

    @Override
    public List<GroupMember<MetricBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public MetricBackendGroup useDefaultGroup() {
        return new Group(backends.useDefault(), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroup(final String group) {
        return new Group(backends.use(group), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroups(Set<String> groups) {
        return new Group(backends.use(groups), metadata.useGroups(groups));
    }

    @ToString
    private class Group extends AbstractMetricBackend implements MetricBackendGroup {
        private final SelectedGroup<MetricBackend> backends;
        private final MetadataBackend metadata;

        public Group(final SelectedGroup<MetricBackend> backends, final MetadataBackend metadata) {
            super(async);
            this.backends = backends;
            this.metadata = metadata;
        }

        @Override
        public Groups getGroups() {
            return backends.groups();
        }

        @Override
        public boolean isEmpty() {
            return backends.isEmpty();
        }

        @Override
        public int size() {
            return backends.size();
        }

        @Override
        public AsyncFuture<AnalyzeResult> analyze(final FullQuery query) {
            /* groupLoadLimit + 1, so that we return one too many results when more than
             * groupLoadLimit series are available. This will cause the query engine to reject the
             * request because of too large group. */
            final RangeFilter rangeFilter =
                RangeFilter.filterFor(query.getFilter(), Optional.of(query.getRange()),
                    seriesLimit + 1);

            final QueryTrace.Tracer tracer = QueryTrace.trace(ANALYZE);
            final Expression.Scope scope = new DefaultScope(query.getNow());

            final LazyTransform<FindSeries, AnalyzeResult> transform =
                (final FindSeries result) -> {
                    final List<AggregationState> input =
                        emptyStates(result.getSeries(), query.getSource(), query.getRange(),
                            query.getOptions());

                    final AggregationContext context = AggregationContext
                        .tracing(async, input, query.getRange(), e -> e.eval(scope))
                        .withOptions(query.getOptions())
                        .withCadence(query.getCadence());

                    return query.getAggregation().setup(context).directTransform(out -> {
                        return AnalyzeResult.analyze(out, tracer::end, ImmutableList.of());
                    });
                };

            return metadata
                .findSeries(rangeFilter)
                .onDone(reporter.reportFindSeries())
                .lazyTransform(transform);
        }

        @Override
        public AsyncFuture<QueryResult> query(final FullQuery query) {
            final FetchQuotaWatcher watcher = new LimitedFetchQuotaWatcher(dataLimit);

            /* groupLoadLimit + 1, so that we return one too many results when more than
             * groupLoadLimit series are available. This will cause the query engine to reject the
             * request because of too large group. */
            final RangeFilter rangeFilter =
                RangeFilter.filterFor(query.getFilter(), Optional.of(query.getRange()),
                    seriesLimit + 1);

            final QueryTrace.Tracer tracer = QueryTrace.trace(QUERY);

            final Expression.Scope scope = new DefaultScope(query.getNow());

            final LazyTransform<FindSeries, QueryResult> transform = (final FindSeries result) -> {
                /* if empty, there are not time series on this shard */
                if (result.isEmpty()) {
                    return async.resolved(QueryResult.empty(tracer.end()));
                }

                if (result.getSize() >= seriesLimit) {
                    throw new IllegalArgumentException(
                        "The total number of series fetched " + result.getSize() +
                            " would exceed the allowed limit of " +
                            seriesLimit);
                }

                final List<AggregationState> input =
                    states(result.getSeries(), query.getSource(), query.getRange(), watcher,
                        query.getOptions());

                final AggregationContext context = AggregationContext
                    .of(async, input, query.getRange(), e -> e.eval(scope))
                    .withOptions(query.getOptions())
                    .withCadence(query.getCadence());

                return query.getAggregation().setup(context).lazyTransform(out -> {
                    if (out.states().size() > groupLimit) {
                        throw new IllegalArgumentException(
                            "The current query is too heavy! (More than " + groupLimit + " " +
                                "timeseries would be sent to your client).");
                    }

                    out.estimate().ifPresent(estimate -> {
                        if (estimate > aggregationLimit) {
                            throw new IllegalArgumentException(String.format(
                                "aggregation is estimated more points [%d/%d] than what is allowed",
                                estimate, aggregationLimit));
                        }
                    });

                    final List<Observable<AggregationData>> observables =
                        new ArrayList<>(out.states().size());

                    for (final AggregationState s : out.states()) {
                        observables.add(s
                            .getObservable()
                            .transform(d -> new AggregationData(s.getKey(), s.getSeries(), d,
                                out.range())));
                    }

                    final ResolvableFuture<QueryResult> future = async.future();

                    Observable.concurrently(observables).observe(new Observer<AggregationData>() {
                        final ConcurrentLinkedQueue<AggregationData> results =
                            new ConcurrentLinkedQueue<>();

                        @Override
                        public void observe(final AggregationData d) throws Exception {
                            results.add(d);
                        }

                        @Override
                        public void fail(final Throwable cause) throws Exception {
                            future.fail(cause);
                        }

                        @Override
                        public void end() throws Exception {
                            final List<AggregationData> groups = ImmutableList.copyOf(results);

                            future.resolve(
                                new QueryResult(out.cadence(), groups, ImmutableList.of(),
                                    Statistics.empty(), tracer.end()));
                        }
                    });

                    return future;
                });
            };

            return metadata
                .findSeries(rangeFilter)
                .onDone(reporter.reportFindSeries())
                .lazyTransform(transform)
                .onDone(reporter.reportQueryMetrics());
        }

        private List<AggregationState> emptyStates(
            final Set<Series> series, final MetricType source, final DateRange range,
            final QueryOptions options
        ) {
            final List<AggregationState> states = new ArrayList<>(series.size());

            for (final Series s : series) {
                states.add(AggregationState.forSeries(s, Observable.empty()));
            }

            return states;
        }

        private List<AggregationState> states(
            final Set<Series> series, final MetricType source, final DateRange range,
            final FetchQuotaWatcher watcher, final QueryOptions options
        ) {
            final List<AggregationState> states = new ArrayList<>(series.size());

            for (final Series s : series) {
                states.add(state(source, s, range, watcher, options));
            }

            return states;
        }

        @Override
        public Statistics getStatistics() {
            Statistics result = Statistics.empty();

            for (final Statistics s : run(b -> b.getStatistics())) {
                result = result.merge(s);
            }

            return result;
        }

        @Override
        public AsyncObservable<FetchData> fetch(
            final MetricType source, final Series series, final DateRange range,
            final FetchQuotaWatcher watcher, final QueryOptions options
        ) {
            final List<AsyncObservable<FetchData>> callbacks =
                run(b -> b.fetch(source, series, range, watcher, options));
            return AsyncObservable.chain(callbacks);
        }

        @Override
        public AsyncObservable<FetchData> fetch(
            final MetricType source, final Series series, final DateRange range,
            final QueryOptions options
        ) {
            return fetch(source, series, range, NO_QUOTA_WATCHER, options);
        }

        @Override
        public AsyncFuture<WriteResult> write(final WriteMetric write) {
            return async
                .collect(run(b -> b.write(write)), WriteResult.merger())
                .onDone(reporter.reportWrite());
        }

        /**
         * Perform a direct write on available configured backends.
         *
         * @param writes Batch of writes to perform.
         * @return A callback indicating how the writes went.
         */
        @Override
        public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
            return async
                .collect(run(b -> b.write(writes)), WriteResult.merger())
                .onDone(reporter.reportWriteBatch());
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeys(
            final BackendKeyFilter filter, final QueryOptions options
        ) {
            return AsyncObservable.chain(run(b -> b.streamKeys(filter, options)));
        }

        @Override
        public boolean isReady() {
            for (final MetricBackend backend : backends) {
                if (!backend.isReady()) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public Iterable<BackendEntry> listEntries() {
            throw new NotImplementedException("not supported");
        }

        @Override
        public AsyncFuture<Void> configure() {
            return async.collectAndDiscard(run(b -> b.configure()));
        }

        @Override
        public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
            return async.collect(run(b -> b.serializeKeyToHex(key))).directTransform(result -> {
                return ImmutableList.copyOf(Iterables.concat(result));
            });
        }

        @Override
        public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
            return async.collect(run(b -> b.deserializeKeyFromHex(key))).directTransform(result -> {
                return ImmutableList.copyOf(Iterables.concat(result));
            });
        }

        @Override
        public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
            return async.collectAndDiscard(run(b -> b.deleteKey(key, options)));
        }

        @Override
        public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
            return async.collect(run(b -> b.countKey(key, options))).directTransform(result -> {
                long count = 0;

                for (final long c : result) {
                    count += c;
                }

                return count;
            });
        }

        @Override
        public AsyncFuture<MetricCollection> fetchRow(final BackendKey key) {
            final List<AsyncFuture<MetricCollection>> callbacks = run(b -> b.fetchRow(key));

            return async.collect(callbacks, (Collection<MetricCollection> results) -> {
                final List<List<? extends Metric>> collections = new ArrayList<>();

                for (final MetricCollection result : results) {
                    collections.add(result.getData());
                }

                return MetricCollection.mergeSorted(key.getType(), collections);
            });
        }

        @Override
        public AsyncObservable<MetricCollection> streamRow(final BackendKey key) {
            return AsyncObservable.chain(run(b -> b.streamRow(key)));
        }

        private void runVoid(InternalOperation<Void> op) {
            for (final MetricBackend b : backends.getMembers()) {
                try {
                    op.run(b);
                } catch (final Exception e) {
                    throw new RuntimeException("setting up backend operation failed", e);
                }
            }
        }

        private <T> List<T> run(InternalOperation<T> op) {
            final ImmutableList.Builder<T> result = ImmutableList.builder();

            for (final MetricBackend b : backends) {
                try {
                    result.add(op.run(b));
                } catch (final Exception e) {
                    throw new RuntimeException("setting up backend operation failed", e);
                }
            }

            return result.build();
        }

        private AggregationState state(
            final MetricType source, final Series series, final DateRange range,
            final FetchQuotaWatcher watcher, final QueryOptions options
        ) {
            final List<AsyncObservable<MetricCollection>> fetches = new ArrayList<>();

            runVoid(b -> {
                fetches.add(observer -> {
                    b
                        .fetch(source, series, range, watcher, options)
                        .observe(new AsyncObserver<FetchData>() {
                            @Override
                            public AsyncFuture<Void> observe(final FetchData value)
                                throws Exception {
                                for (final MetricCollection c : value.getGroups()) {
                                    observer.observe(c);
                                }

                                return async.resolved();
                            }

                            @Override
                            public void fail(final Throwable cause) throws Exception {
                                observer.fail(cause);
                            }

                            @Override
                            public void end() throws Exception {
                                observer.end();
                            }
                        });
                });

                return null;
            });

            return new AggregationState(series.getTags(), ImmutableList.of(series),
                AsyncObservable.chain(fetches).toSync(async));
        }
    }

    private interface InternalOperation<T> {
        T run(MetricBackend backend) throws Exception;
    }
}
