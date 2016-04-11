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

package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.MetricsCollector;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
@Data
public class FilterAggregation implements Aggregation {
    private final FilterStrategy filterStrategy;
    private final Optional<Expression> reference;

    public FilterAggregation(
        final FilterStrategy filterStrategy, final Optional<Expression> reference
    ) {
        this.filterStrategy = checkNotNull(filterStrategy, "filterStrategy");
        this.reference = reference;
    }

    @Override
    public boolean referential() {
        return reference.isPresent();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext input) {
        return input.lookupContext(reference).directTransform(context -> {
            final ImmutableList.Builder<AggregationState> out = ImmutableList.builder();

            // Assume output observable's may only be used once.
            final ConcurrentLinkedQueue<NextState> collected = new ConcurrentLinkedQueue<>();
            final ConcurrentLinkedQueue<Observer<?>> ignored = new ConcurrentLinkedQueue<>();
            final AtomicInteger errors = new AtomicInteger();
            final AtomicInteger pending = new AtomicInteger(context.states().size());

            for (final AggregationState s : context.states()) {
                final Observable<MetricCollection> source = s.getObservable();

                out.add(s.withObservable(observer -> {
                    source.observe(
                        new SubSetObservable(observer, pending, errors, collected, ignored));
                }));
            }

            return context
                .withStep(getClass().getSimpleName(), ImmutableList.of(context.step()),
                    context.step().keys())
                .withStates(out.build());
        });
    }

    @Data
    static class NextState {
        private final MetricCollection data;
        private final Observer<MetricCollection> out;
    }

    @RequiredArgsConstructor
    class SubSetObservable implements Observer<MetricCollection>, MetricsCollector {
        private final Observer<MetricCollection> target;
        private final AtomicInteger pending;
        private final AtomicInteger errors;

        private final ConcurrentLinkedQueue<NextState> collected;
        private final ConcurrentLinkedQueue<Observer<?>> ignored;

        final Queue<List<Point>> points = new ConcurrentLinkedQueue<>();
        volatile boolean any = false;

        @Override
        public void observe(final MetricCollection c) throws Exception {
            any = true;
            c.update(this);
        }

        @Override
        public void fail(final Throwable cause) throws Exception {
            target.fail(cause);
            errors.incrementAndGet();

            if (pending.decrementAndGet() != 0) {
                return;
            }

            doEnd();
        }

        @Override
        public void end() throws Exception {
            if (any) {
                final MetricCollection c = MetricCollection.points(
                    ImmutableList.copyOf(Iterables.concat(points).iterator()));

                collected.add(new NextState(c, target));
            } else {
                ignored.add(target);
            }

            if (pending.decrementAndGet() != 0) {
                return;
            }

            doEnd();
        }

        @Override
        public void collectPoints(final List<Point> values) {
            points.add(values);
        }

        @Override
        public void collectEvents(final List<Event> values) {
            /* ignore events */
        }

        @Override
        public void collectSpreads(final List<Spread> values) {
            /* ignore spreads */
        }

        @Override
        public void collectGroups(final List<MetricGroup> values) {
            /* ignore groups */
        }

        private void doEnd() throws Exception {
            if (errors.get() > 0) {
                for (final NextState s : collected) {
                    s.getOut().end();
                }

                return;
            }

            final List<FilterableMetrics<NextState>> filterable = collected
                .stream()
                .map(d -> new FilterableMetrics<>(d, () -> d.getData()))
                .collect(Collectors.toList());

            for (final NextState d : filterStrategy.filter(filterable)) {
                d.getOut().observe(d.getData());
            }

            for (final NextState d : collected) {
                d.getOut().end();
            }

            for (final Observer<?> o : ignored) {
                o.end();
            }
        }
    }
}
