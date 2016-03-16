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
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricCollectionBiFunction;
import com.spotify.heroic.metric.MetricCollectionFunction;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

@RequiredArgsConstructor
public abstract class BiFunctionAggregation implements Aggregation {
    private final Optional<Expression> left;
    private final Optional<Expression> right;

    protected abstract double applyPoint(double a, double b);

    private static final AggregationLookup.Visitor<Double> toDouble =
        new AggregationLookup.Visitor<Double>() {
            @Override
            public Double visitContext(
                final AsyncFuture<AggregationContext> context
            ) {
                throw new IllegalStateException("Context not expected");
            }

            @Override
            public Double visitExpression(final Expression e) {
                return e.cast(Double.class);
            }
        };

    private static final AggregationLookup.Visitor<AsyncFuture<AggregationContext>> toContext =
        new AggregationLookup.Visitor<AsyncFuture<AggregationContext>>() {
            @Override
            public AsyncFuture<AggregationContext> visitContext(
                final AsyncFuture<AggregationContext> context
            ) {
                return context;
            }

            @Override
            public AsyncFuture<AggregationContext> visitExpression(final Expression e) {
                throw new IllegalStateException("Expression not expected");
            }
        };

    @Override
    public boolean referential() {
        return left.isPresent() || right.isPresent();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext context) {
        final AggregationLookup leftContext = context.lookup(this.left);
        final AggregationLookup rightContext = context.lookup(this.right);

        // nothing to operate on
        if (leftContext.isExpression() && rightContext.isExpression()) {
            return context
                .async()
                .resolved(context
                    .withStep()
                    .withName(getClass().getSimpleName())
                    .withInput(ImmutableList.of())
                    .withParents(ImmutableList.of(context)));
        }

        if (!leftContext.isExpression() && !rightContext.isExpression()) {
            return handleNonLiterals(context, leftContext, rightContext);
        }

        // at least one literal expression
        return handleOneLiteral(context, leftContext, rightContext);
    }

    private AsyncFuture<AggregationContext> handleOneLiteral(
        final AggregationContext context, final AggregationLookup leftContext,
        final AggregationLookup rightContext
    ) {
        final AsyncFuture<AggregationContext> in;
        final Function<Double, Double> function;

        if (leftContext.isExpression()) {
            in = rightContext.visit(toContext);
            final double value = leftContext.visit(toDouble);
            function = p -> applyPoint(value, p);
        } else {
            in = leftContext.visit(toContext);
            final double value = rightContext.visit(toDouble);
            function = p -> applyPoint(p, value);
        }

        return in.directTransform(c -> {
            final List<AggregationState> output = new ArrayList<>(c.input().size());

            for (final AggregationState r : c.input()) {
                output.add(new AggregationState(r.getKey(), r.getSeries(),
                    new BiFunctionLiteralObservable(r.getObservable(), function)));
            }

            return context
                .withStep()
                .withName(getClass().getSimpleName())
                .withInput(output)
                .withParents(ImmutableList.of(c));
        });
    }

    private AsyncFuture<AggregationContext> handleNonLiterals(
        final AggregationContext context, final AggregationLookup leftLookup,
        final AggregationLookup rightLookup
    ) {
        final List<AsyncFuture<AggregationContext>> collected =
            ImmutableList.of(leftLookup.visit(toContext), rightLookup.visit(toContext));

        return context
            .async()
            .collect(collected)
            .directTransform((Collection<AggregationContext> contexts) -> {
                final Iterator<AggregationContext> it = contexts.iterator();
                final AggregationContext left = it.next();
                final AggregationContext right = it.next();

                final Map<Map<String, String>, AggregationState> states = new HashMap<>();

                final List<AggregationState> output = new ArrayList<>();

                for (final AggregationState s : left.input()) {
                    states.put(s.getKey(), s);
                }

                for (final AggregationState r : right.input()) {
                    final AggregationState l = states.remove(r.getKey());

                    if (l == null) {
                        continue;
                    }

                    final Set<Series> series = new HashSet<>();
                    l.getSeries().forEach(series::add);
                    r.getSeries().forEach(series::add);

                    output.add(new AggregationState(r.getKey(), series,
                        new BiFunctionObservable(left.range(), l, right.range(), r)));
                }

                return context
                    .withStep()
                    .withName(getClass().getSimpleName())
                    .withInput(output)
                    .withParents(ImmutableList.of(left, right));
            });
    }

    @RequiredArgsConstructor
    class BiFunctionObservable implements Observable<MetricCollection>, MetricCollectionBiFunction {
        private final DateRange leftRange;
        private final AggregationState l;
        private final DateRange rightRange;
        private final AggregationState r;

        volatile MetricCollection left = null;
        volatile MetricCollection right = null;

        @Override
        public void observe(
            final Observer<MetricCollection> observer
        ) throws Exception {
            final List<Observable<Runnable>> parts =
                ImmutableList.of(l.getObservable().transform(m -> () -> left = m),
                    r.getObservable().transform(m -> () -> right = m));

            Observable.chain(parts).observe(new Observer<Runnable>() {
                @Override
                public void observe(final Runnable action) throws Exception {
                    action.run();
                }

                @Override
                public void fail(final Throwable cause) throws Exception {
                    observer.fail(cause);
                }

                @Override
                public void end() throws Exception {
                    final MetricCollection c = left.apply(BiFunctionObservable.this, right);

                    if (!c.isEmpty()) {
                        observer.observe(c);
                    }

                    observer.end();
                }
            });
        }

        @Override
        public List<Point> applyPoints(
            final List<Point> a, final List<Point> b
        ) {
            return merge(a, b, (ap, bp) -> new Point(Math.max(ap.getTimestamp(), bp.getTimestamp()),
                applyPoint(ap.getValue(), bp.getValue())));
        }

        @Override
        public List<Spread> applySpreads(
            final List<Spread> a, final List<Spread> b
        ) {
            throw new IllegalStateException();
        }

        @Override
        public List<Event> applyEvents(
            final List<Event> a, final List<Event> b
        ) {
            throw new IllegalStateException();
        }

        @Override
        public List<MetricGroup> applyGroups(
            final List<MetricGroup> a, final List<MetricGroup> b
        ) {
            throw new IllegalStateException();
        }

        private <T extends Metric> List<T> merge(
            final List<T> leftData, final List<T> rightData, final BiFunction<T, T, T> function
        ) {
            final Iterator<T> leftIterator = leftData.iterator();
            final Iterator<T> rightIterator = rightData.iterator();

            final List<T> metrics = new ArrayList<>();

            while (true) {
                if (!leftIterator.hasNext()) {
                    break;
                }

                final T left = leftIterator.next();

                if (!rightIterator.hasNext()) {
                    break;
                }

                T right = rightIterator.next();

                final long leftTimestamp = left.getTimestamp() - leftRange.start();
                final long rightTimestamp = right.getTimestamp() - rightRange.start();

                while (rightTimestamp < leftTimestamp) {
                    if (!rightIterator.hasNext()) {
                        break;
                    }

                    right = rightIterator.next();
                }

                if (leftTimestamp != rightTimestamp) {
                    continue;
                }

                metrics.add(function.apply(left, right));
            }

            return metrics;
        }
    }

    @RequiredArgsConstructor
    class BiFunctionLiteralObservable
        implements Observable<MetricCollection>,
        MetricCollectionFunction<Optional<MetricCollection>> {
        private final Observable<MetricCollection> observable;
        private final Function<Double, Double> function;

        @Override
        public void observe(
            final Observer<MetricCollection> observer
        ) throws Exception {
            observable.observe(new Observer<MetricCollection>() {
                @Override
                public void observe(final MetricCollection value) throws Exception {
                    value.apply(BiFunctionLiteralObservable.this).ifPresent(v -> {
                        try {
                            observer.observe(v);
                        } catch (final Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                @Override
                public void fail(final Throwable cause) throws Exception {
                    observer.fail(cause);
                }

                @Override
                public void end() throws Exception {
                    observer.end();
                }
            });
        }

        @Override
        public Optional<MetricCollection> applyPoints(final List<Point> m) {
            final List<Point> points = new ArrayList<>();

            for (final Point p : m) {
                points.add(new Point(p.getTimestamp(), function.apply(p.getValue())));
            }

            return Optional.of(MetricCollection.points(points));
        }

        @Override
        public Optional<MetricCollection> applySpreads(final List<Spread> m) {
            return Optional.empty();
        }

        @Override
        public Optional<MetricCollection> applyEvents(final List<Event> m) {
            return Optional.empty();
        }

        @Override
        public Optional<MetricCollection> applyGroups(
            final List<MetricGroup> m
        ) {
            return Optional.empty();
        }
    }
}
