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
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.DefaultAggregationContext;
import com.spotify.heroic.aggregation.DistributedAggregationCombiner;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.ExpressionScope;
import com.spotify.heroic.grammar.FunctionExpression;
import com.spotify.heroic.grammar.IntegerExpression;
import com.spotify.heroic.grammar.LetExpression;
import com.spotify.heroic.grammar.PlusExpression;
import com.spotify.heroic.grammar.QueryExpression;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.ReferenceExpression;
import com.spotify.heroic.grammar.StringExpression;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryResultPart;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class CoreQueryManager implements QueryManager {
    public static final long SHIFT_TOLERANCE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
    public static final QueryTrace.Identifier QUERY_NODE =
        QueryTrace.identifier(CoreQueryManager.class, "query_node");
    public static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(CoreQueryManager.class, "query");

    private final Features features;
    private final AsyncFramework async;
    private final ClusterManager cluster;
    private final QueryParser parser;
    private final QueryCache queryCache;
    private final AggregationFactory aggregations;
    private final OptionalLimit groupLimit;

    @Inject
    public CoreQueryManager(
        @Named("features") final Features features, final AsyncFramework async,
        final ClusterManager cluster, final QueryParser parser, final QueryCache queryCache,
        final AggregationFactory aggregations, @Named("groupLimit") final OptionalLimit groupLimit
    ) {
        this.features = features;
        this.async = async;
        this.cluster = cluster;
        this.parser = parser;
        this.queryCache = queryCache;
        this.aggregations = aggregations;
        this.groupLimit = groupLimit;
    }

    @Override
    public QueryManager.Group useOptionalGroup(final Optional<String> group) {
        return new Group(cluster.useOptionalGroup(group));
    }

    @Override
    public QueryBuilder newQuery() {
        return new QueryBuilder();
    }

    @RequiredArgsConstructor
    public class Group implements QueryManager.Group {
        private final List<ClusterShard> shards;

        @Override
        public AsyncFuture<QueryResult> query(Query q) {
            final Map<ReferenceExpression, Expression> variables = new HashMap<>();
            final List<Expression> others = new ArrayList<>();

            q.getExpressions().ifPresent(expressions -> {
                expressions.forEach(e -> {
                    e.visit(new Expression.Visitor<Void>() {
                        @Override
                        public Void visitLet(final LetExpression e) {
                            variables.put(e.getReference(), e.getExpression());
                            return null;
                        }

                        @Override
                        public Void defaultAction(final Expression e) {
                            others.add(e);
                            return null;
                        }
                    });
                });
            });

            final ExpressionScope scope = new ExpressionScope(variables);

            if (others.size() > 1) {
                throw new IllegalArgumentException("Only one expression allowed");
            }

            final Optional<QueryExpression> queryExpression =
                others.stream().findFirst().map(resolveQueryExpression(scope));

            final MetricType source = queryExpression
                .flatMap(QueryExpression::getSource)
                .orElseGet(() -> q.getSource().orElse(MetricType.POINT));

            final Aggregation aggregation = queryExpression
                .flatMap(this::queryExpressionToAggregation)
                .map(f -> aggregations.build(f.getName(), f.getArguments(), f.getKeywords()))
                .orElseGet(() -> q.getAggregation().orElse(Empty.INSTANCE));

            final QueryDateRange range = queryExpression
                .flatMap(this::queryExpressionToRange)
                .orElseGet(() -> q
                    .getRange()
                    .orElseThrow(() -> new IllegalArgumentException("range must be present")));

            final Filter filter =
                queryExpression.flatMap(QueryExpression::getFilter).orElseGet(TrueFilter::get);

            final QueryOptions options = q.getOptions().orElseGet(QueryOptions::defaults);
            final Features features = CoreQueryManager.this.features.combine(q.getFeatures());
            final long now = System.currentTimeMillis();

            return query(source, aggregation, filter, range, options, features, now, scope);
        }

        private Function<Expression, QueryExpression> resolveQueryExpression(
            final ExpressionScope scope
        ) {
            return expr -> expr.visit(new Expression.Visitor<QueryExpression>() {
                @Override
                public QueryExpression visitQuery(final QueryExpression e) {
                    return e;
                }

                @Override
                public QueryExpression visitReference(final ReferenceExpression e) {
                    return scope.lookup(e).visit(this);
                }
            });
        }

        private Optional<QueryDateRange> queryExpressionToRange(QueryExpression expr) {
            return expr.getRange().map(e -> {
                final long start = e.getStart().cast(IntegerExpression.class).getValue();
                final long end = e.getEnd().cast(IntegerExpression.class).getValue();

                return new QueryDateRange.Absolute(start, end);
            });
        }

        private Optional<FunctionExpression> queryExpressionToAggregation(
            final QueryExpression expr
        ) {
            return expr.getSelect().map(e -> e.visit(new Expression.Visitor<FunctionExpression>() {
                @Override
                public FunctionExpression visitPlus(final PlusExpression e) {
                    return new FunctionExpression(e.getContext(), "plus",
                        ImmutableList.of(e.getLeft(), e.getRight()), ImmutableMap.of());
                }

                @Override
                public FunctionExpression visitFunction(final FunctionExpression e) {
                    return new FunctionExpression(e.getContext(), "single", ImmutableList.of(e),
                        ImmutableMap.of());
                }

                @Override
                public FunctionExpression visitString(final StringExpression e) {
                    return visitFunction(e.cast(FunctionExpression.class));
                }
            }));
        }

        private AsyncFuture<QueryResult> query(
            final MetricType source, final Aggregation aggregation, final Filter filter,
            final QueryDateRange queryDateRange, final QueryOptions options,
            final Features features, final long now, final ExpressionScope scope
        ) {
            final DateRange range = queryDateRange.buildDateRange(now);
            final AggregationContext context =
                new DefaultAggregationContext(cadenceFromRange(range));
            final AggregationInstance root = aggregation.apply(context);

            final AggregationInstance aggregationInstance;

            if (features.hasFeature(Feature.DISTRIBUTED_AGGREGATIONS)) {
                aggregationInstance = root.distributed();
            } else {
                aggregationInstance = root;
            }

            final DateRange shiftedRange = features.withFeature(Feature.SHIFT_RANGE,
                () -> buildShiftedRange(range, aggregationInstance.cadence(), now), () -> range);

            final FullQuery.Request request =
                new FullQuery.Request(source, filter, shiftedRange, aggregationInstance, options);

            final AggregationCombiner combiner;

            if (features.hasFeature(Feature.DISTRIBUTED_AGGREGATIONS)) {
                combiner = new DistributedAggregationCombiner(root.reducer(), shiftedRange);
            } else {
                combiner = AggregationCombiner.DEFAULT;
            }

            return runDistributed(request, combiner);
        }

        private AsyncFuture<QueryResult> runDistributed(
            final FullQuery.Request request, final AggregationCombiner combiner
        ) {
            return queryCache.load(request, () -> {
                final List<AsyncFuture<QueryResultPart>> futures = new ArrayList<>();

                for (final ClusterShard shard : shards) {
                    final AsyncFuture<QueryResultPart> queryPart = shard
                        .apply(g -> g.query(request))
                        .catchFailed(FullQuery.shardError(QUERY_NODE, shard))
                        .directTransform(QueryResultPart.fromResultGroup(shard));

                    futures.add(queryPart);
                }

                final OptionalLimit limit = request.getOptions().getGroupLimit().orElse(groupLimit);

                return async.collect(futures,
                    QueryResult.collectParts(QUERY, request.getRange(), combiner, limit));
            });
        }

        @Override
        public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
            return run(g -> g.findTags(request), FindTags::shardError, FindTags.reduce());
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
            return run(g -> g.findKeys(request), FindKeys::shardError, FindKeys.reduce());
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
            return run(g -> g.findSeries(request), FindSeries::shardError,
                FindSeries.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
            return run(g -> g.deleteSeries(request), DeleteSeries::shardError,
                DeleteSeries.reduce());
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
            return run(g -> g.countSeries(request), CountSeries::shardError, CountSeries.reduce());
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
            return run(g -> g.tagKeyCount(request), TagKeyCount::shardError,
                TagKeyCount.reduce(request.getLimit(), request.getExactLimit()));
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
            return run(g -> g.tagSuggest(request), TagSuggest::shardError,
                TagSuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
            return run(g -> g.keySuggest(request), KeySuggest::shardError,
                KeySuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            final TagValuesSuggest.Request request
        ) {
            return run(g -> g.tagValuesSuggest(request), TagValuesSuggest::shardError,
                TagValuesSuggest.reduce(request.getLimit(), request.getGroupLimit()));
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
            return run(g -> g.tagValueSuggest(request), TagValueSuggest::shardError,
                TagValueSuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
            return run(g -> g.writeSeries(request), WriteMetadata::shardError,
                WriteMetadata.reduce());
        }

        @Override
        public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request write) {
            return run(g -> g.writeMetric(write), WriteMetric::shardError, WriteMetric.reduce());
        }

        @Override
        public List<ClusterShard> shards() {
            return shards;
        }

        private <T> AsyncFuture<T> run(
            final Function<ClusterNode.Group, AsyncFuture<T>> function,
            final Function<ClusterShard, Transform<Throwable, T>> catcher,
            final Collector<T, T> collector
        ) {
            final List<AsyncFuture<T>> futures = new ArrayList<>(shards.size());

            for (final ClusterShard shard : shards) {
                futures.add(shard.apply(function::apply).catchFailed(catcher.apply(shard)));
            }

            return async.collect(futures, collector);
        }
    }

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

    private Duration cadenceFromRange(final DateRange range) {
        final long diff = range.diff();
        final long nominal = diff / INTERVAL_GOAL;

        final SortedSet<Long> results = INTERVAL_FACTORS.headSet(nominal);

        if (results.isEmpty()) {
            return Duration.of(Math.max(nominal, 1L), TimeUnit.MILLISECONDS);
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
}
