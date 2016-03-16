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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.MetricCollection;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

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
            final List<Observable<MetricCollection>> observables = new ArrayList<>();
            final List<Iterable<Series>> series = new ArrayList<>();

            for (final AggregationState s : context.input()) {
                observables.add(s.getObservable());
                series.add(s.getSeries());
            }

            final Observable<MetricCollection> observable = observer -> {
                Observable.chain(observables).observe(new Observer<MetricCollection>() {
                    final Queue<MetricCollection> data = new ConcurrentLinkedQueue<>();

                    @Override
                    public void observe(final MetricCollection c) throws Exception {
                        data.add(c);
                    }

                    @Override
                    public void fail(final Throwable cause) throws Exception {
                        observer.fail(cause);
                    }

                    @Override
                    public void end() throws Exception {
                        final List<FilterableMetrics<MetricCollection>> filterable = data
                            .stream()
                            .map(d -> new FilterableMetrics<>(d, () -> d))
                            .collect(Collectors.toList());

                        for (final MetricCollection d : filterStrategy.filter(filterable)) {
                            observer.observe(d);
                        }

                        observer.end();
                    }
                });
            };

            final List<AggregationState> states = ImmutableList.of(
                new AggregationState(ImmutableMap.of(), Iterables.concat(series), observable));

            return context
                .withStep()
                .withName(getClass().getSimpleName())
                .withParents(ImmutableList.of(context))
                .withInput(states);
        });
    }
}
