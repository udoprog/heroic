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

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.QueryTrace;
import eu.toolchain.async.AsyncFramework;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@ToString
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TracingAggregationContext implements AggregationContext {
    private final Step step;
    private final AtomicInteger counter;
    private final AsyncFramework async;
    private final List<QueryTrace> traces;
    private final List<NodeError> errors;
    private final Function<Expression, AggregationLookup> lookup;
    private final QueryOptions options;
    private final List<AggregationState> input;
    private final DateRange range;
    private final Optional<Duration> size;
    private final Set<String> requiredTags;
    private final Optional<Long> estimate;
    private final Function<Expression, Expression> evaluator;

    @Override
    public AsyncFramework async() {
        return async;
    }

    @Override
    public Expression eval(final Expression expression) {
        return evaluator.apply(expression);
    }

    @Override
    public AggregationContext withRange(final DateRange range) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public DateRange range() {
        return range;
    }

    @Override
    public AggregationContext withOptions(final QueryOptions options) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public QueryOptions options() {
        return options;
    }

    @Override
    public AggregationContext withLookup(
        final Function<Expression, AggregationLookup> lookup
    ) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public AggregationLookup lookup(final Optional<Expression> reference) {
        return reference
            .map(lookup::apply)
            .orElseGet(() -> new AggregationLookup.Context(overrides -> async.resolved(this)));
    }

    @Override
    public AggregationContext withSize(final Optional<Duration> size) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public Optional<Duration> size() {
        return size;
    }

    @Override
    public AggregationContext withStep(
        final String name, final List<Step> parents, final List<Map<String, String>> keys
    ) {
        final String id = Integer.toString(counter.incrementAndGet());
        final Step next = new Step(id, name, parents, keys);

        return new TracingAggregationContext(next, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public Step step() {
        return step;
    }

    @Override
    public AggregationContext withStates(final List<AggregationState> states) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            states, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public List<AggregationState> states() {
        return input;
    }

    @Override
    public AggregationContext withRequiredTags(final Set<String> requiredTags) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public Set<String> requiredTags() {
        return requiredTags;
    }

    @Override
    public AggregationContext withEstimate(final Optional<Long> estimate) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public Optional<Long> estimate() {
        return estimate;
    }

    @Override
    public AggregationContext withErrors(
        final List<NodeError> errors
    ) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public List<NodeError> errors() {
        return errors;
    }

    @Override
    public AggregationContext withTraces(
        final List<QueryTrace> trace
    ) {
        return new TracingAggregationContext(step, counter, async, traces, errors, lookup, options,
            input, range, size, requiredTags, estimate, evaluator);
    }

    @Override
    public List<QueryTrace> traces() {
        return traces;
    }
}
