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

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.FunctionExpression;
import com.spotify.heroic.grammar.ListExpression;
import com.spotify.heroic.grammar.MinusExpression;
import com.spotify.heroic.grammar.PlusExpression;
import com.spotify.heroic.grammar.StringExpression;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class CoreAggregationFactory implements AggregationFactory {
    final Map<String, AggregationDSL> builderMap;

    @Override
    public Aggregation build(String name, ListExpression args, Map<String, Expression> keywords) {
        final AggregationDSL builder = builderMap.get(name);

        if (builder == null) {
            throw new MissingAggregation(name);
        }

        final AggregationArguments a = new AggregationArguments(args.getList(), keywords);

        final Aggregation aggregation;

        try {
            aggregation = builder.build(a);
        } catch (final Exception e) {
            throw new RuntimeException(name + ": " + e.getMessage(), e);
        }

        // throw an exception unless all provided arguments have been consumed.
        a.throwUnlessEmpty(name);
        return aggregation;
    }

    @Override
    public Optional<Aggregation> fromExpression(final Expression e) {
        return e.visit(new Expression.Visitor<Optional<Aggregation>>() {
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
