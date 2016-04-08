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

package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationLookup;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.DefaultScope;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.QueryExpression;
import com.spotify.heroic.grammar.ReferenceExpression;
import com.spotify.heroic.metric.AnalyzeResult;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class CoreQueryManagerGroup implements QueryManager.Group {
    public static final long SHIFT_TOLERANCE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

    public static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(CoreQueryManagerGroup.class, "query");
    public static final QueryTrace.Identifier ANALYZE =
        QueryTrace.identifier(CoreQueryManagerGroup.class, "analyze");

    private static final SortedSet<Long> INTERVAL_FACTORS =
        ImmutableSortedSet.of(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(50, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(250, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(500, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(15, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES),
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES),
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES),
            TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES),
            TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES),
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS),
            TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS),
            TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS),
            TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS),
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS),
            TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS));

    public static final long INTERVAL_GOAL = 240;

    private final AsyncFramework async;
    private final FilterFactory filters;
    private final AggregationFactory aggregations;
    private final QueryCache queryCache;
    private final Set<String> features;

    private final Function<Expression, QueryInstance> expressionToQuery;
    private final Iterable<ClusterNode.Group> groups;

    @Override
    public AsyncFuture<QueryResult> query(QueryInstance query) {
        return queryCache.load(query, () -> {
            final QueryTrace.Tracer tracer = QueryTrace.trace(QUERY);

            final AsyncFuture<AggregationContext> r = executeQuery(query, this::distributeQuery,
                inner -> newQueryContext(inner, ImmutableList.of()));

            return r.lazyTransform(out -> {
                final List<Observable<AggregationData>> observables = new ArrayList<>();

                for (final AggregationState s : out.states()) {
                    observables.add(s
                        .getObservable()
                        .transform(
                            m -> new AggregationData(s.getKey(), s.getSeries(), m, out.range())));
                }

                final ResolvableFuture<QueryResult> result = async.future();

                Observable
                    .chain(observables)
                    .observe(new ResultObserver(() -> tracer.end(out.traces()), out.errors(),
                        out.cadence(), result, Statistics.empty()));

                return result;
            });
        });
    }

    @Override
    public AsyncFuture<AnalyzeResult> analyze(QueryInstance query) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(ANALYZE);

        final AsyncFuture<AggregationContext> r = executeQuery(query, this::distributeAnalyze,
            inner -> newTracingContext(inner, ImmutableList.of()));

        return r.directTransform(
            result -> AnalyzeResult.analyze(result, () -> tracer.end(result.traces()),
                result.errors(), ImmutableMap.of(), ImmutableList.of()));
    }

    @Override
    public ClusterNode.Group first() {
        return groups.iterator().next();
    }

    @Override
    public Iterator<ClusterNode.Group> iterator() {
        return groups.iterator();
    }

    @RequiredArgsConstructor
    class QueryLookup implements Function<Expression, AggregationLookup> {
        private final QueryInstance parent;
        private final Function<QueryInstance, AsyncFuture<AggregationContext>> query;

        @Override
        public AggregationLookup apply(final Expression reference) {
            return aggregations
                .fromExpression(reference)
                .map(aggregation -> childQuery(
                    new QueryInstance(Optional.of(parent)).withAggregation(aggregation),
                    reference.toString()))
                .orElseGet(() -> lookupExpression(reference));
        }

        private AggregationLookup lookupExpression(final Expression reference) {
            return reference.visit(new Expression.Visitor<AggregationLookup>() {
                @Override
                public AggregationLookup visitReference(final ReferenceExpression e) {
                    final QueryInstance statement = parent
                        .lookupStatement(e.getName())
                        .orElseThrow(() -> new IllegalArgumentException("No such reference: " + e));

                    return childQuery(statement.withParent(parent), e.toString());
                }

                @Override
                public AggregationLookup visitQuery(final QueryExpression e) {
                    return childQuery(expressionToQuery.apply(e).withParent(parent), e.toString());
                }

                @Override
                public AggregationLookup defaultAction(final Expression e) {
                    return new AggregationLookup.Expression(e);
                }
            });
        }

        private AggregationLookup childQuery(final QueryInstance queryInstance, final String name) {
            final QueryTrace.Tracer tracer = QueryTrace.trace(QueryTrace.identifier(name));

            return new AggregationLookup.Context(overrides -> {
                return query
                    .apply(queryInstance.withAddedRangeModifier(overrides.getRange()))
                    .directTransform(context -> {
                        return context.withTraces(ImmutableList.of(tracer.end(context.traces())));
                    });
            });
        }
    }

    private AsyncFuture<AggregationContext> executeQuery(
        final QueryInstance query,
        final Function<QueryInstance, AsyncFuture<AggregationContext>> distribute,
        final Function<QueryInstance, AggregationContext> newContext
    ) {
        final Aggregation aggregation = query.getAggregation().orElse(Empty.INSTANCE);

        if (!aggregation.referential()) {
            return distribute.apply(query);
        }

        final QueryOptions options = query.lookupOptions().orElseGet(QueryOptions::defaults);

        return aggregation.setup(newContext
            .apply(query)
            .withOptions(options)
            .withLookup(
                new QueryLookup(query, inner -> executeQuery(inner, distribute, newContext))));
    }

    /**
     * Construct a new aggregation context for the current query.
     *
     * @param query Query to construct context for.
     * @param states Input states to the aggregation context.
     * @return
     */
    private AggregationContext newQueryContext(
        final QueryInstance query, final List<AggregationState> states
    ) {
        return newContext(query,
            (range, scope) -> AggregationContext.of(async, states, range, e -> e.eval(scope)));
    }

    private AggregationContext newTracingContext(
        final QueryInstance query, final List<AggregationState> states
    ) {
        return newContext(query,
            (range, scope) -> AggregationContext.tracing(async, states, range, e -> e.eval(scope)));
    }

    private AggregationContext newContext(
        final QueryInstance query,
        final BiFunction<DateRange, Expression.Scope, AggregationContext> builder
    ) {
        final DateRange rawRange = query.lookupRange();
        final Duration cadence =
            buildCadence(query.getAggregation().flatMap(Aggregation::cadence), rawRange);
        final DateRange range = rawRange.rounded(cadence.toMilliseconds());
        final Expression.Scope scope = new DefaultScope(query.lookupNow());

        return builder.apply(range, scope).withCadence(cadence);
    }

    private FullQuery newFullQuery(final QueryInstance query) {
        final DateRange rawRange = query.lookupRange();
        final Optional<Duration> c = query.getAggregation().flatMap(Aggregation::cadence);
        final Duration cadence = buildCadence(c, rawRange);
        final DateRange range =
            buildShiftedRange(rawRange, cadence.toMilliseconds(), query.lookupNow());

        final MetricType source = query.lookupSource().orElse(MetricType.POINT);
        final Filter filter = query.lookupCompositeFilter(filters);
        final Aggregation aggregation = query.getAggregation().orElse(Empty.INSTANCE);
        final QueryOptions options = query.lookupOptions().orElseGet(QueryOptions::defaults);

        return new FullQuery(source, filter, range, aggregation, options, cadence,
            query.lookupFeatures(), query.lookupNow());
    }

    private <T> AsyncFuture<AggregationContext> distribute(
        final QueryInstance q,
        final BiFunction<ClusterNode.Group, FullQuery, AsyncFuture<T>> doQuery,
        final BiFunction<Aggregation, Function<List<AggregationState>, AggregationContext>,
            LazyTransform<Collection<T>, AggregationContext>> transform,
        final BiFunction<QueryInstance, List<AggregationState>, AggregationContext> newContext
    ) {
        final QueryInstance query;
        final Aggregation combiner;

        if (features.contains(HeroicFeatures.DISTRIBUTED_AGGREGATIONS) ||
            q.hasFeature(HeroicFeatures.DISTRIBUTED_AGGREGATIONS)) {
            combiner =
                q.getAggregation().map(Aggregation::combiner).orElseGet(Empty.INSTANCE::combiner);
            query = q.withAggregation(q
                .getAggregation()
                .map(Aggregation::distributed)
                .orElseGet(Empty.INSTANCE::distributed));
        } else {
            combiner = Empty.INSTANCE;
            query = q;
        }

        final List<AsyncFuture<T>> futures = new ArrayList<>();

        final FullQuery full = newFullQuery(query);

        for (ClusterNode.Group group : groups) {
            futures.add(doQuery.apply(group, full));
        }

        return async
            .collect(futures)
            .lazyTransform(transform.apply(combiner, states -> newContext.apply(q, states)));
    }

    private AsyncFuture<AggregationContext> distributeQuery(final QueryInstance q) {
        return distribute(q, (group, full) -> {
            final QueryTrace.Identifier identifier = QueryTrace.identifier(group.node().toString());

            return group
                .query(full)
                .catchFailed(QueryResult.nodeError(identifier, group))
                .directTransform(QueryResult.trace(identifier));
        }, QueryTransform::new, this::newQueryContext);
    }

    private AsyncFuture<AggregationContext> distributeAnalyze(QueryInstance q) {
        return distribute(q, (group, full) -> {
            final QueryTrace.Identifier identifier = QueryTrace.identifier(group.node().toString());

            return group
                .analyze(full)
                .catchFailed(AnalyzeResult.nodeError(identifier, group))
                .directTransform(AnalyzeResult.step(identifier));
        }, AnalyzeTransform::new, this::newTracingContext);
    }

    private Duration buildCadence(final Optional<Duration> duration, final DateRange range) {
        return duration.orElseGet(() -> cadenceFromRange(range));
    }

    private Duration cadenceFromRange(final DateRange range) {
        final long diff = range.diff();
        final long nominal = diff / INTERVAL_GOAL;

        final SortedSet<Long> results = INTERVAL_FACTORS.headSet(nominal);

        if (results.isEmpty()) {
            return Duration.of(nominal, TimeUnit.MILLISECONDS);
        }

        return Duration.of(results.last(), TimeUnit.MILLISECONDS);
    }

    /**
     * Given a range and a cadence, return a range that might be shifted in case the end period is
     * too close or after 'now'. This is useful to avoid querying non-complete buckets.
     *
     * @param rawRange Original range.
     * @return A possibly shifted range.
     */
    DateRange buildShiftedRange(DateRange rawRange, long cadence, long now) {
        if (rawRange.getStart() > now) {
            throw new IllegalArgumentException("start is greater than now");
        }

        final DateRange rounded = rawRange.rounded(cadence);

        final long nowDelta = now - rounded.getEnd();

        if (nowDelta > SHIFT_TOLERANCE) {
            return rounded;
        }

        final long diff = Math.abs(Math.min(nowDelta, 0)) + SHIFT_TOLERANCE;

        return rounded.shift(-toleranceShiftPeriod(diff, cadence));
    }

    /**
     * Calculate a tolerance shift period that corresponds to the given difference that needs to be
     * applied to the range to honor the tolerance shift period.
     *
     * @param diff The time difference to apply.
     * @param cadence The cadence period.
     * @return The number of milliseconds that the query should be shifted to get within 'now' and
     * maintain the given cadence.
     */
    private long toleranceShiftPeriod(final long diff, final long cadence) {
        // raw query, only shift so that we are within now.
        if (cadence <= 0L) {
            return diff;
        }

        // Round up periods
        return ((diff + cadence - 1) / cadence) * cadence;
    }

    @RequiredArgsConstructor
    static class QueryTransform
        implements LazyTransform<Collection<QueryResult>, AggregationContext> {
        private final Aggregation combiner;
        private final Function<List<AggregationState>, AggregationContext> contextBuilder;

        @Override
        public AsyncFuture<AggregationContext> transform(
            final Collection<QueryResult> all
        ) throws Exception {
            final Optional<Duration> cadence =
                all.stream().findAny().flatMap(QueryResult::getCadence);

            final List<AggregationState> states = new ArrayList<>();
            final List<NodeError> errors = new ArrayList<>();
            final List<QueryTrace> traces = new ArrayList<>();
            Statistics statistics = Statistics.empty();

            for (final QueryResult r : all) {
                r.toStates().forEach(states::add);
                errors.addAll(r.getErrors());
                traces.add(r.getTrace());
                statistics = statistics.merge(r.getStatistics());
            }

            return combiner
                .setup(contextBuilder.apply(states))
                .directTransform(ctx -> ctx.withErrors(errors).withTraces(traces));
        }
    }

    @RequiredArgsConstructor
    static class ResultObserver implements Observer<AggregationData> {
        final Supplier<QueryTrace> trace;
        final List<NodeError> errors;

        final Optional<Duration> cadence;
        final ResolvableFuture<QueryResult> result;
        final Statistics statistics;

        final Queue<AggregationData> results = new ConcurrentLinkedQueue<>();

        @Override
        public void observe(final AggregationData value) throws Exception {
            results.add(value);
        }

        @Override
        public void fail(final Throwable cause) throws Exception {
            result.fail(cause);
        }

        @Override
        public void end() throws Exception {
            result.resolve(
                new QueryResult(cadence, ImmutableList.copyOf(results), errors, statistics,
                    trace.get()));
        }
    }

    @RequiredArgsConstructor
    static class AnalyzeTransform
        implements LazyTransform<Collection<AnalyzeResult>, AggregationContext> {
        private final Aggregation combiner;
        private final Function<List<AggregationState>, AggregationContext> contextBuilder;

        @Override
        public AsyncFuture<AggregationContext> transform(final Collection<AnalyzeResult> all)
            throws Exception {
            final List<AggregationState> states = new ArrayList<>();
            final List<NodeError> errors = new ArrayList<>();
            final List<QueryTrace> traces = new ArrayList<>();

            for (final AnalyzeResult result : all) {
                result.toStates().forEach(states::add);
                errors.addAll(result.getErrors());
                traces.add(result.getTrace());
            }

            return combiner
                .setup(contextBuilder.apply(states))
                .directTransform(ctx -> ctx.withErrors(errors).withTraces(traces));
        }
    }
}
