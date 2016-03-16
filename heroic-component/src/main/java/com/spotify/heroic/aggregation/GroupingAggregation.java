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
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.grammar.Expression;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
@EqualsAndHashCode(of = {"of", "each", "reference"})
public abstract class GroupingAggregation implements Aggregation {
    private final Optional<List<String>> of;
    private final Aggregation each;
    private final Optional<Expression> reference;

    public GroupingAggregation(
        final Optional<List<String>> of, final Optional<Aggregation> each,
        final Optional<Expression> reference
    ) {
        this.of = checkNotNull(of, "of");
        this.each = each.orElse(Empty.INSTANCE);
        this.reference = reference;
    }

    /**
     * Generate a key for the specific group.
     *
     * @param input The input tags for the group.
     * @return The keys for a specific group.
     */
    protected abstract Map<String, String> key(Map<String, String> input, Optional<Set<String>> of);

    @Override
    public boolean referential() {
        return reference.isPresent() || each.referential();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(AggregationContext input) {
        return input.lookupContext(reference).lazyTransform(context -> {
            final Optional<Set<String>> of = this.of.map(o -> {
                ImmutableSet.Builder<String> result = ImmutableSet.builder();
                result.addAll(o);
                result.addAll(context.requiredTags());
                return result.build();
            });

            final Map<Map<String, String>, List<AggregationState>> mappings =
                map(context.input(), of);

            final List<AggregationState> output = new ArrayList<>();

            final AggregationContext parent = context
                .withStep()
                .withName(name(of) + " (in)")
                .withParents(ImmutableList.of(context));

            final List<AsyncFuture<Pair<AggregationContext, Map<String, String>>>> children =
                new ArrayList<>();

            for (final Map.Entry<Map<String, String>, List<AggregationState>> e : mappings
                .entrySet()) {
                children.add(each
                    .setup(parent.withInput(e.getValue()))
                    .directTransform(ctx -> Pair.of(ctx, e.getKey())));
            }

            return context.async().collect(children).directTransform(pairs -> {
                final ImmutableList.Builder<AggregationContext> parents = ImmutableList.builder();

                for (final Pair<AggregationContext, Map<String, String>> pair : pairs) {
                    final AggregationContext out = pair.getLeft();
                    final Map<String, String> key = pair.getRight();

                    final List<AggregationState> modified = new ArrayList<>(out.input().size());

                    for (final AggregationState s : out.input()) {
                        final AggregationState m = s.withKey(key);
                        modified.add(m);
                        output.add(m);
                    }

                    parents.add(out.withInput(modified));
                }

                final AggregationContext end;

                if (context.options().isCompactTracing()) {
                    final AggregationContext step = context
                        .withStep()
                        .withName(each.getClass().getSimpleName())
                        .withParents(ImmutableList.of(parent));

                    end = parent
                        .withStep()
                        .withName(name(of) + " (out)")
                        .withParents(ImmutableList.of(step));
                } else {
                    end = parent
                        .withStep()
                        .withName(name(of) + " (out)")
                        .withParents(parents.build());
                }

                return end.withInput(output);
            });
        });
    }

    @Override
    public Aggregation distributed() {
        return new Group(of, Optional.of(each.distributed()), Optional.empty());
    }

    @Override
    public Aggregation combiner() {
        return new Group(of, Optional.of(each.combiner()), Optional.empty());
    }

    @Override
    public Set<String> requiredTags() {
        return of.map(ImmutableSet::copyOf).orElseGet(ImmutableSet::of);
    }

    String name(Optional<Set<String>> of) {
        return getClass().getSimpleName() + "{of=" + of.map(Object::toString).orElse("<empty>") +
            "}";
    }

    /**
     * Traverse the given input states, and map them to their corresponding keys.
     *
     * @param states Input states to map.
     * @return A mapping for the group key, to a set of series.
     */
    Map<Map<String, String>, List<AggregationState>> map(
        final List<AggregationState> states, final Optional<Set<String>> of
    ) {
        final Map<Map<String, String>, List<AggregationState>> output = new HashMap<>();

        for (final AggregationState s : states) {
            final Map<String, String> k = key(s.getKey(), of);

            List<AggregationState> mapping = output.get(k);

            if (mapping == null) {
                mapping = new ArrayList<>();
                output.put(k, mapping);
            }

            mapping.add(s);
        }

        return output;
    }
}
