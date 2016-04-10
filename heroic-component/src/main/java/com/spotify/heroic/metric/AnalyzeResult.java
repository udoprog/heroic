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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.metric.QueryTrace.Identifier;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@Data
public final class AnalyzeResult {
    public static final List<AggregationData> EMPTY_DATA = ImmutableList.of();
    public static final Map<String, AnalyzeNode> EMPTY_NODES = ImmutableMap.of();
    public static final List<AnalyzeEdge> EMPTY_EDGES = ImmutableList.of();

    private final String in;
    private final String out;
    private final List<AggregationData> data;
    private final Optional<Duration> cadence;
    private final Map<String, AnalyzeNode> nodes;
    private final List<AnalyzeEdge> edges;
    private final List<NodeError> errors;
    private final QueryTrace trace;

    @JsonCreator
    public AnalyzeResult(
        @JsonProperty("in") final String in, @JsonProperty("out") final String out,
        @JsonProperty("data") List<AggregationData> data,
        @JsonProperty("size") Optional<Duration> cadence,
        @JsonProperty("nodes") Map<String, AnalyzeNode> nodes,
        @JsonProperty("edges") List<AnalyzeEdge> edges,
        @JsonProperty("errors") final List<NodeError> errors,
        @JsonProperty("trace") QueryTrace trace
    ) {
        this.in = Objects.requireNonNull(in, "in");
        this.out = Objects.requireNonNull(out, "out");
        this.data = Objects.requireNonNull(data, "data");
        this.cadence = Objects.requireNonNull(cadence, "size");
        this.nodes = Objects.requireNonNull(nodes, "nodes");
        this.edges = Objects.requireNonNull(edges, "edges");
        this.errors = Objects.requireNonNull(errors, "errors");
        this.trace = Objects.requireNonNull(trace, "trace");
    }

    public Iterable<AggregationState> toStates() {
        return Iterables.transform(data, d -> new AggregationState(d.getKey(), d.getSeries(),
            Observable.ofValues(d.getMetrics())));
    }

    public AnalyzeResult withPrefix(final String prefix) {
        return new AnalyzeResult(prefix + in, prefix + out, data, cadence, nodesWithPrefix(prefix),
            edgesWithPrefix(prefix), errors, trace);
    }

    public Map<String, AnalyzeNode> nodesWithPrefix(final String prefix) {
        final ImmutableMap.Builder<String, AnalyzeNode> nodes = ImmutableMap.builder();

        // modify edges and nodes
        for (final Map.Entry<String, AnalyzeResult.AnalyzeNode> e : this.nodes.entrySet()) {
            nodes.put(prefix + e.getKey(), e.getValue());
        }

        return nodes.build();
    }

    public List<AnalyzeEdge> edgesWithPrefix(final String prefix) {
        final ImmutableList.Builder<AnalyzeEdge> edges = ImmutableList.builder();

        for (final AnalyzeResult.AnalyzeEdge e : this.edges) {
            edges.add(e.withPrefix(prefix));
        }

        return edges.build();
    }

    @Data
    public static class AnalyzeNode {
        private final String name;

        @JsonCreator
        public AnalyzeNode(
            @JsonProperty("name") final String name
        ) {
            this.name = name;
        }
    }

    @Data
    public static class AnalyzeEdge {
        private final String from;
        private final String to;
        private final List<Map<String, String>> keys;

        @JsonCreator
        public AnalyzeEdge(
            @JsonProperty("from") final String from, @JsonProperty("to") final String to,
            @JsonProperty("keys") final List<Map<String, String>> keys
        ) {
            this.from = from;
            this.to = to;
            this.keys = keys;
        }

        public AnalyzeEdge withPrefix(final String prefix) {
            return new AnalyzeEdge(prefix + from, prefix + to, keys);
        }
    }

    public static Collector<AnalyzeResult, AnalyzeResult> collect(final Identifier what) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(what);

        return results -> {
            Optional<Duration> cadence = Optional.empty();

            final ImmutableList.Builder<AggregationData> data = ImmutableList.builder();
            final ImmutableMap.Builder<String, AnalyzeNode> nodes = ImmutableMap.builder();
            final ImmutableList.Builder<AnalyzeEdge> edges = ImmutableList.builder();
            final ImmutableList.Builder<NodeError> errors = ImmutableList.builder();
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();

            for (final AnalyzeResult r : results) {
                cadence = Optionals.firstPresent(cadence, r.getCadence());
                data.addAll(r.getData());
                nodes.putAll(r.nodes);
                edges.addAll(r.edges);
                errors.addAll(r.errors);
                traces.add(r.trace);
            }

            return new AnalyzeResult(AggregationContext.IN, AggregationContext.OUT, data.build(),
                cadence, nodes.build(), edges.build(), errors.build(), tracer.end(traces.build()));
        };
    }

    public static AnalyzeResult analyze(
        final AggregationContext out, final Supplier<QueryTrace> trace, final List<NodeError> errors
    ) {
        return analyze(out, trace, errors, ImmutableMap.of(), ImmutableList.of());
    }

    public static AnalyzeResult analyze(
        final AggregationContext out, final Supplier<QueryTrace> trace,
        final List<NodeError> errors, final Map<String, AnalyzeNode> startingNodes,
        final List<AnalyzeEdge> startingEdges
    ) {
        final ImmutableMap.Builder<String, AnalyzeNode> nodes = ImmutableMap.builder();
        nodes.putAll(startingNodes);

        final ImmutableList.Builder<AnalyzeEdge> edges = ImmutableList.builder();
        edges.addAll(startingEdges);

        final Queue<AggregationContext.Step> queue = new LinkedList<>();
        queue.add(out.step());
        nodes.put(out.step().id(), new AnalyzeResult.AnalyzeNode(out.step().name()));

        final List<Map<String, String>> initial =
            out.states().stream().map(s -> s.getKey()).collect(Collectors.toList());

        final Set<String> seen = new HashSet<>();

        nodes.put(AggregationContext.OUT, new AnalyzeResult.AnalyzeNode(AggregationContext.OUT));
        edges.add(new AnalyzeResult.AnalyzeEdge(out.step().id(), AggregationContext.OUT, initial));

        while (!queue.isEmpty()) {
            final AggregationContext.Step node = queue.poll();

            for (final AggregationContext.Step p : node.parents()) {
                edges.add(new AnalyzeResult.AnalyzeEdge(p.id(), node.id(), p.keys()));

                if (!seen.add(p.id())) {
                    continue;
                }

                queue.add(p);
                nodes.put(p.id(), new AnalyzeResult.AnalyzeNode(p.name()));
            }
        }

        final ImmutableList.Builder<AggregationData> data = ImmutableList.builder();

        for (final AggregationState s : out.states()) {
            data.add(new AggregationData(s.getKey(), s.getSeries(), MetricCollection.empty(),
                out.range()));
        }

        return new AnalyzeResult(AggregationContext.IN, AggregationContext.OUT, data.build(),
            out.size(), nodes.build(), edges.build(), errors, trace.get());
    }

    public static Transform<Throwable, AnalyzeResult> nodeError(
        final Identifier what, final ClusterNode.Group group
    ) {
        return (Throwable e) -> {
            final List<NodeError> errors =
                ImmutableList.<NodeError>of(NodeError.fromThrowable(group.node(), e));
            return new AnalyzeResult("", "", EMPTY_DATA, Optional.empty(), EMPTY_NODES, EMPTY_EDGES,
                errors, new QueryTrace(what));
        };
    }

    public static Transform<AnalyzeResult, AnalyzeResult> trace(final Identifier what) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(what);

        return r -> new AnalyzeResult(r.in, r.out, r.data, r.cadence, r.nodes, r.edges, r.errors,
            tracer.end(r.trace));
    }

    public static Transform<AnalyzeResult, AnalyzeResult> step(final Identifier identifier) {
        final QueryTrace.Tracer tracer = QueryTrace.trace(identifier);

        return r -> new AnalyzeResult(r.in, r.out, r.data, r.cadence, r.nodes, r.edges, r.errors,
            tracer.end(r.getTrace()));
    }

    public void writeGraphviz(PrintWriter out) {
        out.println("digraph query {");

        for (final Map.Entry<String, AnalyzeNode> e : nodes.entrySet()) {
            out.println(
                String.format("  id_%s [label=%s];", e.getKey(), gvEscape(e.getValue().getName())));
        }

        for (final AnalyzeEdge e : edges) {
            out.println(String.format("  id_%s -> id_%s [label=%s, labeltooltip=%s];", e.getFrom(),
                e.getTo(), keysName(e.getKeys()), tooltip(e.getKeys())));
        }

        out.println("}");
    }

    static Joiner keyJoiner = Joiner.on(", ");
    static Joiner entryJoiner = Joiner.on("&#10;");

    private String tooltip(final List<Map<String, String>> keys) {
        if (keys.size() > 6) {
            return gvEscape("<" + keys.size() + ">");
        }

        return gvEscape(entryJoiner.join(keys.stream().map(k -> keyName(k)).iterator()));
    }

    private String keysName(final List<Map<String, String>> keys) {
        if (keys.size() == 1 && keys.get(0).isEmpty()) {
            return gvEscape("{empty}");
        }

        return gvEscape("<" + keys.size() + ">");
    }

    private String keyName(final Map<String, String> k) {
        if (k.isEmpty()) {
            return "{empty}";
        }

        return keyJoiner.join(k.entrySet().stream().map(e -> gvEscape(e.getKey()) + "=" +
            gvEscape(e.getValue())).iterator());
    }

    static String gvEscape(String input) {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            switch (c) {
                case '\b':
                    builder.append("\\b");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\'':
                    builder.append("\\'");
                    break;
                default:
                    builder.append(c);
                    break;
            }
        }

        return "\"" + builder.toString() + "\"";
    }
}
