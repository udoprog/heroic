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

import com.google.common.base.Stopwatch;
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
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.DefaultScope;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.QueryExpression;
import com.spotify.heroic.grammar.RangeExpression;
import com.spotify.heroic.grammar.ReferenceExpression;
import com.spotify.heroic.metric.AnalyzeResult;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RequestError;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class CoreQueryManagerGroup implements QueryManager.Group {
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
    private final long now;

    private final Function<Expression, QueryInstance> expressionToQuery;
    private final Iterable<ClusterNode.Group> groups;

    @Override
    public AsyncFuture<QueryResult> query(QueryInstance query) {
        return queryCache.load(query, () -> {
            final QueryTrace.Tracer tracer = QueryTrace.trace(QUERY);

            return queryComplex(query).lazyTransform(out -> {
                final List<Observable<AggregationData>> observables = new ArrayList<>();

                for (final AggregationState s : out.input()) {
                    observables.add(s
                        .getObservable()
                        .transform(
                            m -> new AggregationData(s.getKey(), s.getSeries(), m, out.range())));
                }

                final ResolvableFuture<QueryResult> result = async.future();

                Observable
                    .chain(observables)
                    .observe(new ResultObserver(() -> tracer.end(out.traces()), result,
                        ImmutableList.of(), Statistics.empty()));

                return result;
            });
        });
    }

    @Override
    public AsyncFuture<AnalyzeResult> analyze(QueryInstance query) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(ANALYZE);

        return complexAnalyze(query).directTransform(
            result -> AnalyzeResult.analyze(result, () -> tracer.end(result.traces()),
                ImmutableMap.of(), ImmutableList.of(), ImmutableList.of()));
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
        private final Function<QueryInstance, AsyncFuture<AggregationContext>> lookupQuery;

        @Override
        public AggregationLookup apply(final Expression reference) {
            return aggregations
                .fromExpression(reference)
                .map(aggregation -> runQuery(
                    new QueryInstance(ImmutableMap.of(), parent.getSource(), parent.getFilter(),
                        parent.getRange(), aggregation, parent.getOptions(), parent.getCadence(),
                        parent.getFeatures())))
                .orElseGet(() -> reference.visit(new Expression.Visitor<AggregationLookup>() {
                    @Override
                    public AggregationLookup visitReference(
                        final ReferenceExpression e
                    ) {
                        final QueryInstance statement = parent.getStatements().get(e.getName());

                        if (statement == null) {
                            throw new IllegalArgumentException(
                                "No such reference: $" + e.getName());
                        }

                        return runQuery(statement);
                    }

                    @Override
                    public AggregationLookup visitQuery(
                        final QueryExpression e
                    ) {
                        return runQuery(expressionToQuery.apply(e));
                    }

                    @Override
                    public AggregationLookup defaultAction(final Expression e) {
                        return new AggregationLookup.Expression(e);
                    }
                }));
        }

        private AggregationLookup runQuery(final QueryInstance queryInstance) {
            return new AggregationLookup.Context(overrides -> this.lookupQuery.apply(queryInstance
                .andFilter(filters, parent.getFilter())
                .withRangeIfOtherPresent(overrides.getRange())
                .withRangeIfAbsent(parent.getRange())
                .withParentStatements(parent.getStatements())));
        }
    }

    private AsyncFuture<AggregationContext> queryComplex(final QueryInstance query) {
        if (!query.getAggregation().referential()) {
            return distributeQuery(query);
        }

        final Function<Expression, AggregationLookup> lookup =
            new QueryLookup(query, this::queryComplex);

        final AggregationContext context = newDefault(query, ImmutableList.of()).withLookup(lookup);

        return query.getAggregation().setup(context);
    }

    private AsyncFuture<AggregationContext> complexAnalyze(
        final QueryInstance query
    ) {
        if (!query.getAggregation().referential()) {
            return distributeAnalyze(query);
        }

        final Function<Expression, AggregationLookup> lookup =
            new QueryLookup(query, this::complexAnalyze);

        final AggregationContext context = newTracing(query, ImmutableList.of())
            .withOptions(query.getOptions())
            .withLookup(lookup);

        return query.getAggregation().setup(context);
    }

    private AggregationContext newDefault(
        final QueryInstance query, final List<AggregationState> states
    ) {
        final Expression.Scope scope = new DefaultScope(now);

        final DateRange rawRange =
            toDateRange(query.getRange().orElseGet(this::defaultDateRange).eval(scope));
        final Duration cadence = buildCadence(query.getCadence(), rawRange);
        final DateRange range = rawRange.rounded(cadence.toMilliseconds());

        return AggregationContext.of(async, states, range, cadence, e -> e.eval(scope));
    }

    private AggregationContext newTracing(
        final QueryInstance query, final List<AggregationState> states
    ) {
        final Expression.Scope scope = new DefaultScope(now);

        final DateRange rawRange =
            toDateRange(query.getRange().orElseGet(this::defaultDateRange).eval(scope));
        final Duration cadence = buildCadence(query.getCadence(), rawRange);
        final DateRange range = rawRange.rounded(cadence.toMilliseconds());

        return AggregationContext.tracing(async, states, range, cadence, e -> e.eval(scope));
    }

    private FullQuery newFullQuey(final QueryInstance query) {
        final Expression.Scope scope = new DefaultScope(now);

        final DateRange rawRange =
            toDateRange(query.getRange().orElseGet(this::defaultDateRange).eval(scope));
        final Duration cadence = buildCadence(query.getCadence(), rawRange);
        final DateRange range = rawRange.rounded(cadence.toMilliseconds());

        final ImmutableMap.Builder<String, FullQuery> statements = ImmutableMap.builder();

        for (final Map.Entry<String, QueryInstance> e : query.getStatements().entrySet()) {
            statements.put(e.getKey(), newFullQuey(e.getValue()));
        }

        return new FullQuery(statements.build(), query.getSource(), query.getFilter(), range,
            query.getAggregation(), query.getOptions(), cadence, query.getFeatures(), now);
    }

    private DateRange toDateRange(final RangeExpression range) {
        final long start = range.getStart().cast(Long.class);
        final long end = range.getEnd().cast(Long.class);
        return new DateRange(start, end);
    }

    private AsyncFuture<AggregationContext> distributeQuery(final QueryInstance q) {
        final QueryInstance query;
        final Aggregation combiner;

        if (features.contains(Query.DISTRIBUTED_AGGREGATIONS) ||
            q.hasFeature(Query.DISTRIBUTED_AGGREGATIONS)) {
            combiner = q.getAggregation().combiner();
            query = q.withAggregation(q.getAggregation().distributed());
        } else {
            combiner = Empty.INSTANCE;
            query = q;
        }

        final List<AsyncFuture<QueryResult>> futures = new ArrayList<>();

        final FullQuery full = newFullQuey(query);

        for (ClusterNode.Group group : groups) {
            final ClusterNode c = group.node();

            final QueryTrace.Identifier identifier = QueryTrace.identifier(c.toString());

            futures.add(group
                .query(full)
                .catchFailed(QueryResult.nodeError(identifier, group))
                .directTransform(QueryResult.step(identifier)));
        }

        return async
            .collect(futures)
            .lazyTransform(new QueryTransform(combiner, states -> newDefault(q, states)));
    }

    private AsyncFuture<AggregationContext> distributeAnalyze(QueryInstance q) {
        final Stopwatch w = Stopwatch.createStarted();

        final QueryInstance query;
        final Aggregation combiner;

        if (features.contains(Query.DISTRIBUTED_AGGREGATIONS) ||
            q.hasFeature(Query.DISTRIBUTED_AGGREGATIONS)) {
            combiner = q.getAggregation().combiner();
            query = q.withAggregation(q.getAggregation().distributed());
        } else {
            combiner = Empty.INSTANCE;
            query = q;
        }

        final List<AsyncFuture<AnalyzeResult>> futures = new ArrayList<>();

        final FullQuery full = newFullQuey(query);

        for (final ClusterNode.Group group : groups) {
            final ClusterNode c = group.node();

            final QueryTrace.Identifier identifier = QueryTrace.identifier(c.toString());

            futures.add(group
                .analyze(full)
                .catchFailed(AnalyzeResult.nodeError(identifier, group))
                .directTransform(AnalyzeResult.step(identifier)));
        }

        return async
            .collect(futures)
            .lazyTransform(new AnalyzeTransform(combiner, states -> newTracing(q, states)));
    }

    private Duration buildCadence(
        final Optional<Duration> duration, final DateRange range
    ) {
        return duration.orElseGet(() -> cadenceFromRange(range));
    }

    private RangeExpression defaultDateRange() {
        return Expression.range(
            Expression.minus(Expression.reference("now"), Expression.duration(TimeUnit.DAYS, 1)),
            Expression.reference("now"));
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

    @RequiredArgsConstructor
    static class QueryTransform
        implements LazyTransform<Collection<QueryResult>, AggregationContext> {
        private final Aggregation combiner;
        private final Function<List<AggregationState>, AggregationContext> contextBuilder;

        @Override
        public AsyncFuture<AggregationContext> transform(
            final Collection<QueryResult> all
        ) throws Exception {
            final List<AggregationState> states = new ArrayList<>();

            final List<RequestError> errors = new ArrayList<>();
            Statistics statistics = Statistics.empty();
            final List<QueryTrace> traces = new ArrayList<>();

            for (final QueryResult r : all) {
                r.toStates().forEach(states::add);
                errors.addAll(r.getErrors());
                statistics = statistics.merge(r.getStatistics());
                traces.add(r.getTrace());
            }

            return combiner
                .setup(contextBuilder.apply(states))
                .directTransform(ctx -> ctx.withTraces(traces));
        }
    }

    @RequiredArgsConstructor
    static class ResultObserver implements Observer<AggregationData> {
        final Supplier<QueryTrace> trace;

        final ResolvableFuture<QueryResult> result;
        final List<RequestError> errors;
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
                new QueryResult(new ArrayList<>(results), errors, statistics, trace.get()));
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
            final List<QueryTrace> traces = new ArrayList<>();

            for (final AnalyzeResult result : all) {
                result.toStates().forEach(states::add);
                traces.add(result.getTrace());
            }

            return combiner
                .setup(contextBuilder.apply(states))
                .directTransform(ctx -> ctx.withTraces(traces));
        }
    }
}
