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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.DivideExpression;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.FunctionExpression;
import com.spotify.heroic.grammar.ListExpression;
import com.spotify.heroic.grammar.MinusExpression;
import com.spotify.heroic.grammar.MultiplyExpression;
import com.spotify.heroic.grammar.NegateExpression;
import com.spotify.heroic.grammar.PlusExpression;
import com.spotify.heroic.grammar.StringExpression;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@RequiredArgsConstructor
public class CoreAggregationFactory implements AggregationFactory {
    private static final Joiner PATH_JOINER = Joiner.on(" -> ");

    final Map<String, Function<FunctionExpression, FunctionExpression>> aliases;
    final Map<String, Function<AggregationArguments, Aggregation>> builders;

    @Override
    public Aggregation build(String name, ListExpression args, Map<String, Expression> keywords) {
        final FunctionExpression func = resolveFunction(name, args, keywords);

        final Function<AggregationArguments, Aggregation> builder = builders.get(func.getName());

        if (builder == null) {
            throw new MissingAggregation(name);
        }

        final AggregationArguments a = func.arguments();

        final Aggregation aggregation;

        try {
            aggregation = builder.apply(a);
        } catch (final Exception e) {
            throw new RuntimeException(name + ": " + e.getMessage(), e);
        }

        // throw unless all arguments consumed
        a.throwUnlessEmpty(func.getName());
        return aggregation;
    }

    private FunctionExpression resolveFunction(
        final String name, final ListExpression args, final Map<String, Expression> keywords
    ) {
        FunctionExpression func = Expression.aggregation(name, args, keywords);

        final List<FunctionExpression> path = new ArrayList<>();
        final Set<String> seen = new HashSet<>();

        while (true) {
            path.add(func);

            if (!seen.add(func.getName())) {
                throw new IllegalStateException(
                    "Encountered alias cycle (" + PATH_JOINER.join(path) + ")");
            }

            final Function<FunctionExpression, FunctionExpression> alias =
                aliases.get(func.getName());

            if (alias == null) {
                return func;
            }

            func = alias.apply(func);
        }
    }

    @Override
    public Optional<Aggregation> fromExpression(final Expression e) {
        return e.visit(new Expression.Visitor<Optional<Aggregation>>() {
            @Override
            public Optional<Aggregation> visitNegate(final NegateExpression e) {
                return Optional.of(
                    build("negate", Expression.list(e.getExpression()), ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> visitMultiply(final MultiplyExpression e) {
                return Optional.of(build("multiply", Expression.list(e.getLeft(), e.getRight()),
                    ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> visitDivide(final DivideExpression e) {
                return Optional.of(
                    build("divide", Expression.list(e.getLeft(), e.getRight()), ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> visitMinus(final MinusExpression e) {
                return Optional.of(build("subtract", Expression.list(e.getLeft(), e.getRight()),
                    ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> visitPlus(final PlusExpression e) {
                return Optional.of(
                    build("add", Expression.list(e.getLeft(), e.getRight()), ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> visitFunction(final FunctionExpression e) {
                return Optional.of(build(e.getName(), e.getArguments(), e.getKeywordArguments()));
            }

            @Override
            public Optional<Aggregation> visitString(final StringExpression e) {
                return Optional.of(build(e.getString(), Expression.list(), ImmutableMap.of()));
            }

            @Override
            public Optional<Aggregation> defaultAction(final Expression e) {
                return Optional.empty();
            }
        });
    }
}
