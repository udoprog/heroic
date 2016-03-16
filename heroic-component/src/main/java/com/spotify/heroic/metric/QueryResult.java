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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.QueryTrace.Identifier;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Data
public final class QueryResult {
    private static final List<AggregationData> EMPTY_GROUPS = new ArrayList<>();
    public static final List<NodeError> EMPTY_ERRORS = new ArrayList<>();

    private final Optional<Duration> cadence;
    private final List<AggregationData> data;
    private final List<NodeError> errors;
    private final Statistics statistics;
    private final QueryTrace trace;

    @JsonCreator
    public QueryResult(
        @JsonProperty("cadence") Optional<Duration> cadence,
        @JsonProperty("data") List<AggregationData> data,
        @JsonProperty("errors") List<NodeError> errors,
        @JsonProperty("statistics") Statistics statistics, @JsonProperty("trace") QueryTrace trace
    ) {
        this.cadence = Objects.requireNonNull(cadence, "cadence");
        this.data = Objects.requireNonNull(data, "data");
        this.errors = Objects.requireNonNull(errors, "errors");
        this.statistics = Objects.requireNonNull(statistics, "statistics");
        this.trace = Objects.requireNonNull(trace, "trace");
    }

    public Iterable<AggregationState> toStates() {
        return Iterables.transform(data, d -> new AggregationState(d.getKey(), d.getSeries(),
            Observable.ofValues(d.getMetrics())));
    }

    @JsonIgnore
    public boolean isEmpty() {
        return data.stream().allMatch(AggregationData::isEmpty);
    }

    public static QueryResult empty(final QueryTrace trace) {
        return new QueryResult(Optional.empty(), ImmutableList.of(), ImmutableList.of(),
            Statistics.empty(), trace);
    }

    public static Collector<QueryResult, QueryResult> collect(final QueryTrace.Identifier what) {
        final Stopwatch w = Stopwatch.createStarted();

        return results -> {
            final Optional<Duration> cadence =
                results.stream().findAny().flatMap(QueryResult::getCadence);
            final ImmutableList.Builder<AggregationData> data = ImmutableList.builder();
            final ImmutableList.Builder<NodeError> errors = ImmutableList.builder();
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();

            Statistics statistics = Statistics.empty();

            for (final QueryResult r : results) {
                data.addAll(r.data);
                errors.addAll(r.errors);
                traces.add(r.trace);
                statistics = statistics.merge(r.statistics);
            }

            return new QueryResult(cadence, data.build(), errors.build(), statistics,
                new QueryTrace(what, w, traces.build()));
        };
    }

    public static Transform<Throwable, QueryResult> nodeError(
        final QueryTrace.Identifier what, final ClusterNode.Group group
    ) {
        return (Throwable e) -> {
            final List<NodeError> errors =
                ImmutableList.<NodeError>of(NodeError.fromThrowable(group.node(), e));
            return new QueryResult(Optional.empty(), EMPTY_GROUPS, errors, Statistics.empty(),
                new QueryTrace(what));
        };
    }

    public static Transform<QueryResult, QueryResult> trace(final Identifier identifier) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(identifier);

        return r -> new QueryResult(r.cadence, r.data, r.errors, r.statistics,
            tracer.end(ImmutableList.of(r.trace)));
    }
}
