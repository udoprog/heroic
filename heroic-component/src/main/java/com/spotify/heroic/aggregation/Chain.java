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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Duration;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Set;

@Data
public class Chain implements Aggregation {
    public static final String NAME = "chain";
    public static final Joiner PIPE = Joiner.on(" | ");

    private final List<Aggregation> chain;

    public Chain(List<Aggregation> chain) {
        this(Optional.ofNullable(chain));
    }

    @JsonCreator
    public Chain(@JsonProperty("chain") Optional<List<Aggregation>> chain) {
        this.chain = chain
            .filter(c -> !c.isEmpty())
            .orElseThrow(
                () -> new IllegalArgumentException("chain must be specified and non-empty"));
    }

    @Override
    public boolean referential() {
        return chain.stream().anyMatch(Aggregation::referential);
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext context) {
        AsyncFuture<AggregationContext> current = context.async().resolved(context);

        for (final Pair<Set<String>, Aggregation> step : steps(context)) {
            current = current.lazyTransform(
                c -> step.getRight().setup(c.withRequiredTags(step.getLeft())));
        }

        return current;
    }

    @Override
    public Set<String> requiredTags() {
        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();
        chain.stream().map(Aggregation::requiredTags).forEach(tags::addAll);
        return tags.build();
    }

    @Override
    public Aggregation distributed() {
        final List<Aggregation> newChain = new ArrayList<>();
        final Iterator<Aggregation> it = chain.iterator();

        while (it.hasNext()) {
            final Aggregation n = it.next();

            if (!it.hasNext()) {
                newChain.add(n.distributed());
                break;
            }

            newChain.add(n);
        }

        return new Chain(newChain);
    }

    @Override
    public Aggregation combiner() {
        final Iterator<Aggregation> it = chain.iterator();

        while (it.hasNext()) {
            final Aggregation n = it.next();

            if (!it.hasNext()) {
                return n.combiner();
            }
        }

        throw new IllegalStateException("empty chain");
    }

    private List<Pair<Set<String>, Aggregation>> steps(final AggregationContext context) {
        final ListIterator<Aggregation> it = this.chain.listIterator(this.chain.size());

        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();
        tags.addAll(context.requiredTags());

        final List<Pair<Set<String>, Aggregation>> steps = new ArrayList<>();

        while (it.hasPrevious()) {
            final Aggregation aggregation = it.previous();
            tags.addAll(aggregation.requiredTags());
            steps.add(Pair.of(tags.build(), aggregation));
        }

        Collections.reverse(steps);
        return ImmutableList.copyOf(steps);
    }
}
