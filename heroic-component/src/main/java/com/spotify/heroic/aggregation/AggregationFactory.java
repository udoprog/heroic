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

import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.ListExpression;
import com.spotify.heroic.grammar.ReferenceExpression;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Factory to dynamically build aggregations.
 * <p>
 * Used in Query DSL.
 *
 * @author udoprog
 */
public interface AggregationFactory {
    /**
     * Build an aggregation with the given name and arguments.
     *
     * @param name The name of the aggregation.
     * @param args Positional arguments of the aggregation.
     * @param keywords Keyword arguments of the aggregation.
     * @return The built aggregation.
     * @throws MissingAggregation If the given name does not reflect an available aggregation.
     */
    Aggregation build(String name, ListExpression args, Map<String, Expression> keywords);

    /**
     * Build an aggregation from an expression.
     *
     * @param e Expresison to build from.
     * @return A new aggregation built from the given expression.
     */
    Optional<Aggregation> fromExpression(Expression e);

    /**
     * Same as {@link #fromExpression(com.spotify.heroic.grammar.Expression)} but defaults
     * references to the {@link com.spotify.heroic.aggregation.Empty} aggregation.
     *
     * @param e Expression to build aggregation from.
     * @return An new aggregation matching the given expression.
     * @throws java.lang.IllegalArgumentException if the given expression cannot be converted.
     */
    default Aggregation fromExpressionWithEmpty(Expression e) {
        Objects.requireNonNull(e);

        return fromExpression(e).orElseGet(() -> e.visit(new Expression.Visitor<Aggregation>() {
            @Override
            public Aggregation visitReference(final ReferenceExpression e) {
                return new Empty(Optional.of(e));
            }

            @Override
            public Aggregation defaultAction(final Expression e) {
                throw new IllegalArgumentException("Not an aggregation (" + e + ")");
            }
        }));
    }
}
