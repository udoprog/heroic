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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.QueryTrace;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public interface AggregationContext {
    String IN = "in";
    String OUT = "out";

    Step EMPTY = new Step(IN, IN, ImmutableList.of(), ImmutableList.of());

    AsyncFramework async();

    AggregationContext withRange(final DateRange range);

    DateRange range();

    AggregationContext withErrors(final List<NodeError> errors);

    List<NodeError> errors();

    AggregationContext withTraces(final List<QueryTrace> traces);

    List<QueryTrace> traces();

    default AggregationContext withSize(final Duration size) {
        return withSize(Optional.of(size));
    }

    AggregationContext withSize(final Optional<Duration> size);

    Optional<Duration> size();

    AggregationContext withStates(final List<AggregationState> states);

    List<AggregationState> states();

    AggregationContext withRequiredTags(final Set<String> requiredTags);

    Set<String> requiredTags();

    AggregationContext withEstimate(final Optional<Long> estimate);

    Optional<Long> estimate();

    AggregationContext withOptions(final QueryOptions options);

    QueryOptions options();

    AggregationContext withLookup(
        final Function<Expression, AggregationLookup> lookup
    );

    AggregationLookup lookup(final Optional<Expression> reference);

    default AsyncFuture<AggregationContext> lookupContext(Optional<Expression> reference) {
        return lookupContext(reference, LookupOverrides.empty());
    }

    default AsyncFuture<AggregationContext> lookupContext(
        Optional<Expression> reference, LookupOverrides overrides
    ) {
        return lookup(reference).visit(new AggregationLookup.Visitor<LookupFunction>() {
            @Override
            public LookupFunction visitContext(
                final LookupFunction context
            ) {
                return context;
            }

            @Override
            public LookupFunction visitExpression(
                final Expression e
            ) {
                throw new IllegalArgumentException("Unsupported expression: " + e);
            }
        }).apply(overrides);
    }

    default Step step() {
        return EMPTY;
    }

    default AggregationContext withStep(
        final String name, final List<Step> parents, final List<Map<String, String>> keys
    ) {
        return this;
    }

    Expression eval(Expression expression);

    static AggregationContext of(
        final AsyncFramework async, final List<AggregationState> input, final DateRange range,
        final Function<Expression, Expression> evaluator
    ) {
        return new DefaultAggregationContext(async, ImmutableList.of(), ImmutableList.of(),
            new DefaultLookup(), QueryOptions.DEFAULTS, input, range, Optional.empty(),
            ImmutableSet.of(), Optional.empty(), evaluator, ImmutableMap.of());
    }

    static AggregationContext tracing(
        final AsyncFramework async, final List<AggregationState> input, final DateRange range,
        final Function<Expression, Expression> evaluator
    ) {
        final ImmutableList.Builder<Map<String, String>> keys = ImmutableList.builder();

        for (final AggregationState state : input) {
            keys.add(state.getKey());
        }

        final Step root = new Step("in", "in", ImmutableList.of(), keys.build());

        return new TracingAggregationContext(root, new AtomicInteger(), async, ImmutableList.of(),
            ImmutableList.of(), new DefaultLookup(), QueryOptions.DEFAULTS, input, range,
            Optional.empty(), ImmutableSet.of(), Optional.empty(), evaluator, ImmutableMap.of());
    }

    Map<String, String> as();

    AggregationContext withAs(Map<String, String> as);

    @RequiredArgsConstructor
    class DefaultLookup implements Function<Expression, AggregationLookup> {
        @Override
        public AggregationLookup apply(final Expression reference) {
            throw new IllegalStateException("No such reference: " + reference);
        }
    }

    @ToString
    @RequiredArgsConstructor
    class Step {
        private final String id;
        private final String name;
        private final List<Step> parents;
        private final List<Map<String, String>> keys;

        public String id() {
            return id;
        }

        public String name() {
            return name;
        }

        public List<Step> parents() {
            return parents;
        }

        public List<Map<String, String>> keys() {
            return keys;
        }
    }
}
