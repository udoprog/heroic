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
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricCollectionFunction;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public abstract class FunctionAggregation implements Aggregation {
    private final Optional<Expression> reference;

    protected abstract Point applyPoint(Point p);

    @Override
    public boolean referential() {
        return reference.isPresent();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext context) {
        return context.lookupContext(this.reference).directTransform(out -> {
            final List<AggregationState> states =
                new ArrayList<AggregationState>(out.states().size());

            for (final AggregationState in : out.states()) {
                states.add(in.withObservable(new FunctionObservable(in.getObservable())));
            }

            return out
                .withStep(getClass().getSimpleName(), ImmutableList.of(out.step()),
                    out.step().keys())
                .withInput(states);
        });
    }

    @Data
    class FunctionObservable
        implements Observable<MetricCollection>, MetricCollectionFunction<MetricCollection> {
        final Observable<MetricCollection> previous;

        @Override
        public void observe(
            final Observer<MetricCollection> previousObserver
        ) throws Exception {
            previous.observe(new Observer<MetricCollection>() {
                @Override
                public void observe(final MetricCollection value) throws Exception {
                    previousObserver.observe(value.apply(FunctionObservable.this));
                }

                @Override
                public void fail(final Throwable cause) throws Exception {
                    previousObserver.fail(cause);
                }

                @Override
                public void end() throws Exception {
                    previousObserver.end();
                }
            });
        }

        @Override
        public MetricCollection applyPoints(
            final List<Point> m
        ) {
            final List<Point> output = new ArrayList<>(m.size());

            for (final Point p : m) {
                output.add(applyPoint(p));
            }

            return MetricCollection.points(output);
        }

        @Override
        public MetricCollection applySpreads(
            final List<Spread> m
        ) {
            throw new IllegalStateException();
        }

        @Override
        public MetricCollection applyEvents(
            final List<Event> m
        ) {
            throw new IllegalStateException();
        }

        @Override
        public MetricCollection applyGroups(
            final List<MetricGroup> a
        ) {
            throw new IllegalStateException();
        }
    }
}
