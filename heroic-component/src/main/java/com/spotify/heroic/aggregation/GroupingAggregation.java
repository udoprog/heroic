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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;
import com.spotify.heroic.metric.TagValues;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@EqualsAndHashCode(of = { "of", "each" })
public abstract class GroupingAggregation implements AggregationInstance {
    private final Optional<List<String>> of;
    private final AggregationInstance each;

    public GroupingAggregation(final Optional<List<String>> of, final AggregationInstance each) {
        this.of = of;
        this.each = checkNotNull(each, "each");
    }

    /**
     * Generate a key for the specific group.
     *
     * @param input The input tags for the group.
     * @return The keys for a specific group.
     */
    protected abstract Map<String, String> key(final Map<String, String> input);

    /**
     * Create a new instance of this aggregation.
     */
    protected abstract AggregationInstance newInstance(final Optional<List<String>> of,
            final AggregationInstance each);

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        return traversal(map(states), range);
    }

    /**
     * Traverse the given input states, and map them to their corresponding keys.
     *
     * @param states Input states to map.
     * @return A mapping for the group key, to a set of series.
     */
    Map<Map<String, String>, Set<Series>> map(final List<AggregationState> states) {
        final Map<Map<String, String>, Set<Series>> output = new HashMap<>();

        for (final AggregationState state : states) {
            final Map<String, String> k = key(state.getKey());

            Set<Series> series = output.get(k);

            if (series == null) {
                series = new HashSet<Series>();
                output.put(k, series);
            }

            series.addAll(state.getSeries());
        }

        return output;
    }

    /**
     * Setup traversal and the corresponding session from the given mapping.
     *
     * @param mapping The mapping of series.
     * @param range The range to setup child sessions using.
     * @return A new traversal instance.
     */
    AggregationTraversal traversal(final Map<Map<String, String>, Set<Series>> mapping,
            final DateRange range) {
        final Map<Map<String, String>, AggregationSession> sessions = new HashMap<>();
        final List<AggregationState> states = new ArrayList<>();

        for (final Map.Entry<Map<String, String>, Set<Series>> e : mapping.entrySet()) {
            final Set<Series> series = new HashSet<>();

            final AggregationTraversal traversal = each.session(
                    ImmutableList.of(new AggregationState(e.getKey(), e.getValue())), range);

            for (final AggregationState state : traversal.getStates()) {
                series.addAll(state.getSeries());
            }

            sessions.put(e.getKey(), traversal.getSession());
            states.add(new AggregationState(e.getKey(), series));
        }

        return new AggregationTraversal(states, new GroupSession(sessions));
    }

    @Override
    public long estimate(DateRange range) {
        return each.estimate(range);
    }

    @Override
    public long cadence() {
        return each.cadence();
    }

    @Override
    public AggregationInstance distributed() {
        return newInstance(of, each.distributed());
    }

    @Override
    public ReducerSession reducer(final DateRange range) {
        return each.reducer(range);
    }

    @Override
    public AggregationCombiner combiner(final DateRange range) {
        return new AggregationCombiner() {
            @Override
            public List<ShardedResultGroup> combine(final List<List<ShardedResultGroup>> all) {
                final Map<Map<String, String>, Reduction> sessions = new HashMap<>();

                // combine all the tags.
                final Iterator<ShardedResultGroup> step1 =
                        Iterators.concat(Iterators.transform(all.iterator(), Iterable::iterator));

                while (step1.hasNext()) {
                    final ShardedResultGroup g = step1.next();
                    final Map<String, String> key = key(TagValues.mapOfSingles(g.getTags()));

                    Reduction red = sessions.get(key);

                    if (red == null) {
                        red = new Reduction(each.reducer(range));
                        sessions.put(key, red);
                    }

                    g.getGroup().updateReducer(red.session, key);
                    red.tagValues.add(g.getTags().iterator());
                }

                final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

                for (final Map.Entry<Map<String, String>, Reduction> e : sessions.entrySet()) {
                    final Reduction red = e.getValue();
                    final ReducerSession session = red.session;
                    final List<TagValues> tagValues = TagValues.fromEntries(Iterators
                            .concat(Iterators.transform(Iterators.concat(red.tagValues.iterator()),
                                    TagValues::iterator)));

                    for (final MetricCollection metrics : session.result().getResult()) {
                        groups.add(new ShardedResultGroup(e.getKey(), tagValues, metrics,
                                each.cadence()));
                    }
                }

                return groups.build();
            }
        };
    }

    @Override
    public String toString() {
        return String.format("%s(of=%s, each=%s)", getClass().getSimpleName(), of, each);
    }

    @Data
    private final class Reduction {
        private final ReducerSession session;
        private final List<Iterator<TagValues>> tagValues = new ArrayList<>();
    }

    @ToString
    @RequiredArgsConstructor
    private final class GroupSession implements AggregationSession {
        private final Map<Map<String, String>, AggregationSession> sessions;

        @Override
        public void updatePoints(Map<String, String> group, Set<Series> series,
                List<Point> values) {
            session(group).updatePoints(group, series, values);
        }

        @Override
        public void updateEvents(Map<String, String> group, Set<Series> series,
                List<Event> values) {
            session(group).updateEvents(group, series, values);
        }

        @Override
        public void updateSpreads(Map<String, String> group, Set<Series> series,
                List<Spread> values) {
            session(group).updateSpreads(group, series, values);
        }

        @Override
        public void updateGroup(Map<String, String> group, Set<Series> series,
                List<MetricGroup> values) {
            session(group).updateGroup(group, series, values);
        }

        private AggregationSession session(final Map<String, String> group) {
            final Map<String, String> key = key(group);
            final AggregationSession session = sessions.get(key);

            if (session == null) {
                throw new IllegalStateException(
                        String.format("no session for key (%s) derived from %s, has (%s)", key,
                                group, sessions.keySet()));
            }

            return session;
        }

        @Override
        public AggregationResult result() {
            final Map<ResultKey, ResultValues> groups = new HashMap<>();

            Statistics statistics = Statistics.empty();

            for (final Map.Entry<Map<String, String>, AggregationSession> e : sessions.entrySet()) {
                final AggregationResult r = e.getValue().result();
                statistics = statistics.merge(r.getStatistics());

                for (final AggregationData data : r.getResult()) {
                    final MetricCollection metrics = data.getMetrics();
                    final ResultKey key = new ResultKey(e.getKey(), metrics.getType());

                    ResultValues result = groups.get(key);

                    if (result == null) {
                        result = new ResultValues();
                        groups.put(key, result);
                    }

                    result.series.addAll(data.getSeries());
                    result.values.add(metrics.getData());
                }
            }

            final List<AggregationData> data = new ArrayList<>();

            for (final Map.Entry<ResultKey, ResultValues> e : groups.entrySet()) {
                final ResultKey k = e.getKey();
                final ResultValues v = e.getValue();
                final MetricCollection metrics = MetricCollection.mergeSorted(k.type, v.values);
                data.add(new AggregationData(k.group, v.series, metrics));
            }

            return new AggregationResult(data, statistics);
        }
    }

    @Data
    private static class ResultKey {
        final Map<String, String> group;
        final MetricType type;
    }

    private static class ResultValues {
        final Set<Series> series = new HashSet<>();
        final List<List<? extends Metric>> values = new ArrayList<>();
    }
}
