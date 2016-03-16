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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.DefaultScope;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

@Data
@RequiredArgsConstructor
public class QueryInstance {
    private final Optional<QueryInstance> parent;
    private final Map<String, QueryInstance> statements;
    private final Set<String> features;
    private final Optional<MetricType> source;
    private final Optional<Filter> filter;
    private final Optional<Aggregation> aggregation;
    private final Optional<QueryOptions> options;
    private final Optional<Long> now;
    private final Optional<Function<Expression.Scope, DateRange>> rangeBuilder;
    private final Function<DateRange, DateRange> rangeModifier;

    public QueryInstance(final Optional<QueryInstance> parent) {
        this(parent, ImmutableMap.of(), ImmutableSet.of(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Function.identity());
    }

    public boolean hasFeature(final String feature) {
        return features.contains(feature);
    }

    public QueryInstance withNow(final long now) {
        return new QueryInstance(parent, statements, features, source, filter, aggregation, options,
            Optional.of(now), rangeBuilder, rangeModifier);
    }

    public QueryInstance withAddedFeatures(final Set<String> features) {
        final ImmutableSet.Builder<String> result = ImmutableSet.builder();
        result.addAll(this.features).addAll(features);

        return new QueryInstance(parent, statements, result.build(), source, filter, aggregation,
            options, now, rangeBuilder, rangeModifier);
    }

    public QueryInstance withParent(final QueryInstance parent) {
        return new QueryInstance(Optional.of(parent), statements, features, source, filter,
            aggregation, options, now, rangeBuilder, rangeModifier);
    }

    public QueryInstance withAggregation(final Aggregation aggregation) {
        return new QueryInstance(parent, statements, features, source, filter,
            Optional.of(aggregation), options, now, rangeBuilder, rangeModifier);
    }

    public QueryInstance withStatements(final Map<String, QueryInstance> statements) {
        return new QueryInstance(parent, statements, features, source, filter, aggregation, options,
            now, rangeBuilder, rangeModifier);
    }

    public QueryInstance modifyOptions(Function<QueryOptions, QueryOptions> modify) {
        final Optional<QueryOptions> options = Optional.of(
            this.options.map(modify).orElseGet(() -> modify.apply(QueryOptions.defaults())));

        return new QueryInstance(parent, statements, features, source, filter, aggregation, options,
            now, rangeBuilder, rangeModifier);
    }

    public QueryInstance withRangeIfAbsent(
        final Optional<Function<Expression.Scope, DateRange>> rangeBuilder
    ) {
        if (this.rangeBuilder.isPresent()) {
            return this;
        }

        return new QueryInstance(parent, statements, features, source, filter, aggregation, options,
            now, rangeBuilder, rangeModifier);
    }

    public QueryInstance withOptionsIfAbsent(final Optional<QueryOptions> options) {
        if (!this.options.isPresent()) {
            return new QueryInstance(parent, statements, features, source, filter, aggregation,
                options, now, rangeBuilder, rangeModifier);
        }

        return this;
    }

    public QueryInstance withAddedRangeModifier(
        final Function<DateRange, DateRange> rangeModifier
    ) {
        return new QueryInstance(parent, statements, features, source, filter, aggregation, options,
            now, rangeBuilder, this.rangeModifier.andThen(rangeModifier));
    }

    public Optional<Aggregation> getAggregation() {
        return lookup(i -> i.aggregation);
    }

    public long lookupNow() {
        return lookup(i -> i.now).orElseThrow(
            () -> new IllegalStateException("Now time unavailable"));
    }

    public DateRange lookupRange() {
        final long now = lookupNow();

        final Expression.Scope scope = new DefaultScope(now);

        return rangeModifier.apply(lookup(i -> i.rangeBuilder.map(b -> b.apply(scope))).orElseGet(
            () -> defaultDateRange(now)));
    }

    public Optional<QueryInstance> lookupStatement(final String name) {
        return lookup(i -> Optional.ofNullable(i.statements.get(name)));
    }

    public Optional<QueryOptions> lookupOptions() {
        return lookup(i -> i.options);
    }

    public Optional<MetricType> lookupSource() {
        return lookup(i -> i.source);
    }

    public Filter lookupCompositeFilter(final FilterFactory filters) {
        final List<Filter> children = new ArrayList<>();
        tree().forEach(i -> i.filter.ifPresent(children::add));

        if (children.isEmpty()) {
            return filters.t();
        }

        return filters.and(children).optimize();
    }

    public Set<String> lookupFeatures() {
        final ImmutableSet.Builder<String> features = ImmutableSet.builder();
        tree().forEach(i -> features.addAll(i.features));
        return features.build();
    }

    Stream<QueryInstance> tree() {
        final Stream.Builder<QueryInstance> builder = Stream.builder();

        QueryInstance current = this;

        while (true) {
            builder.add(current);

            if (!current.parent.isPresent()) {
                break;
            }

            current = current.parent.get();
        }

        return builder.build();
    }

    /**
     * Recursively lookup the given function up the parent hierarchy.
     *
     * @param resolver Function to lookup using.
     * @param <T> Type to lookup
     * @return An Optional containing the resolved value.
     */
    <T> Optional<T> lookup(Function<QueryInstance, Optional<T>> resolver) {
        QueryInstance current = this;

        while (true) {
            final Optional<T> resolved = resolver.apply(current);

            if (resolved.isPresent()) {
                return resolved;
            }

            if (!current.parent.isPresent()) {
                break;
            }

            current = current.parent.get();
        }

        return Optional.empty();
    }

    private DateRange defaultDateRange(final long now) {
        return new DateRange(now - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS), now);
    }
}
