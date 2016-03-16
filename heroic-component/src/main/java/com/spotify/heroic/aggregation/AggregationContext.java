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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.QueryTrace;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public interface AggregationContext {
    AsyncFramework async();

    String id();

    List<AggregationContext> parents();

    List<AggregationState> input();

    DateRange range();

    Duration cadence();

    Set<String> requiredTags();

    Optional<Long> estimate();

    Optional<String> name();

    QueryOptions options();

    List<QueryTrace> traces();

    AggregationLookup lookup(final Optional<Expression> reference);

    AggregationContext withTraces(final List<QueryTrace> traces);

    AggregationContext withCadence(final Duration cadence);

    AggregationContext withInput(final List<AggregationState> input);

    AggregationContext withRequiredTags(final Set<String> requiredTags);

    AggregationContext withEstimate(final Optional<Long> estimate);

    AggregationContext withOptions(final QueryOptions options);

    AggregationContext withLookup(
        final Function<Expression, AggregationLookup> lookup
    );

    default AsyncFuture<AggregationContext> lookupContext(Optional<Expression> reference) {
        return lookup(reference).visit(
            new AggregationLookup.Visitor<AsyncFuture<AggregationContext>>() {
                @Override
                public AsyncFuture<AggregationContext> visitContext(
                    final AsyncFuture<AggregationContext> context
                ) {
                    return context;
                }

                @Override
                public AsyncFuture<AggregationContext> visitExpression(final Expression e) {
                    throw new IllegalArgumentException("Unsupported expression: " + e);
                }
            });
    }

    default AggregationContext withParents(final List<AggregationContext> parents) {
        return this;
    }

    default AggregationContext withStep() {
        return this;
    }

    default AggregationContext withName(final String name) {
        return this;
    }

    static AggregationContext of(
        final AsyncFramework async, final List<AggregationState> input, final DateRange range,
        final Duration cadence
    ) {
        return new DefaultAggregationContext(async, ImmutableList.of(), new DefaultLookup(),
            QueryOptions.DEFAULTS, input, range, cadence, ImmutableSet.of(), Optional.empty());
    }

    static AggregationContext tracing(
        final AsyncFramework async, final List<AggregationState> input, final DateRange range,
        final Duration cadence
    ) {
        return new TracingAggregationContext(async, ImmutableList.of(), new DefaultLookup(),
            new AtomicInteger(), "in", Optional.empty(), ImmutableList.of(), QueryOptions.DEFAULTS,
            input, range, cadence, ImmutableSet.of(), Optional.empty());
    }

    @ToString
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    class TracingAggregationContext implements AggregationContext {
        private final AsyncFramework async;
        private final List<QueryTrace> traces;
        private final Function<Expression, AggregationLookup> lookup;
        private final AtomicInteger counter;
        private final String id;
        private final Optional<String> name;
        private final List<AggregationContext> parents;
        private final QueryOptions options;
        private final List<AggregationState> input;
        private final DateRange range;
        private final Duration cadence;
        private final Set<String> requiredTags;
        private final Optional<Long> estimate;

        @Override
        public AsyncFramework async() {
            return async;
        }

        @Override
        public QueryOptions options() {
            return options;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public List<AggregationContext> parents() {
            return parents;
        }

        @Override
        public List<AggregationState> input() {
            return input;
        }

        @Override
        public DateRange range() {
            return range;
        }

        @Override
        public Duration cadence() {
            return cadence;
        }

        @Override
        public Set<String> requiredTags() {
            return requiredTags;
        }

        @Override
        public Optional<Long> estimate() {
            return estimate;
        }

        @Override
        public Optional<String> name() {
            return name;
        }

        @Override
        public List<QueryTrace> traces() {
            return traces;
        }

        @Override
        public AggregationLookup lookup(final Optional<Expression> reference) {
            return reference
                .map(lookup::apply)
                .orElseGet(() -> new AggregationLookup.Context(async.resolved(this)));
        }

        @Override
        public AggregationContext withOptions(final QueryOptions options) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withLookup(
            final Function<Expression, AggregationLookup> lookup
        ) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withCadence(final Duration cadence) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withParents(
            final List<AggregationContext> parents
        ) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withStep() {
            final String id = Integer.toString(counter.incrementAndGet());

            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withName(final String name) {
            return new TracingAggregationContext(async, traces, lookup, counter, id,
                Optional.of(name), parents, options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withInput(final List<AggregationState> input) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withRequiredTags(final Set<String> requiredTags) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withEstimate(final Optional<Long> estimate) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withTraces(
            final List<QueryTrace> trace
        ) {
            return new TracingAggregationContext(async, traces, lookup, counter, id, name, parents,
                options, input, range, cadence, requiredTags, estimate);
        }
    }

    @ToString
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    class DefaultAggregationContext implements AggregationContext {
        private final AsyncFramework async;
        private final List<QueryTrace> traces;
        private final Function<Expression, AggregationLookup> lookup;
        private final QueryOptions options;
        private final List<AggregationState> input;
        private final DateRange range;
        private final Duration cadence;
        private final Set<String> requiredTags;
        private final Optional<Long> estimate;

        @Override
        public AsyncFramework async() {
            return async;
        }

        @Override
        public QueryOptions options() {
            return options;
        }

        @Override
        public String id() {
            return "";
        }

        @Override
        public List<AggregationContext> parents() {
            return ImmutableList.of();
        }

        @Override
        public List<AggregationState> input() {
            return input;
        }

        @Override
        public DateRange range() {
            return range;
        }

        @Override
        public Duration cadence() {
            return cadence;
        }

        @Override
        public Set<String> requiredTags() {
            return requiredTags;
        }

        @Override
        public Optional<Long> estimate() {
            return estimate;
        }

        @Override
        public Optional<String> name() {
            return Optional.empty();
        }

        @Override
        public List<QueryTrace> traces() {
            return traces;
        }

        @Override
        public AggregationLookup lookup(final Optional<Expression> reference) {
            return reference
                .map(lookup::apply)
                .orElseGet(() -> new AggregationLookup.Context(async.resolved(this)));
        }

        @Override
        public AggregationContext withOptions(final QueryOptions options) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withLookup(
            final Function<Expression, AggregationLookup> lookup
        ) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withCadence(final Duration cadence) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withInput(final List<AggregationState> input) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withRequiredTags(final Set<String> requiredTags) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withEstimate(final Optional<Long> estimate) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }

        @Override
        public AggregationContext withTraces(
            final List<QueryTrace> traces
        ) {
            return new DefaultAggregationContext(async, traces, lookup, options, input, range,
                cadence, requiredTags, estimate);
        }
    }

    @RequiredArgsConstructor
    class DefaultLookup implements Function<Expression, AggregationLookup> {
        @Override
        public AggregationLookup apply(final Expression reference) {
            throw new IllegalStateException("No such reference: " + reference);
        }
    }
}
