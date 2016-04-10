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

package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.QueryInstance;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.common.DateRange;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Function;

public class CoreExpressionEvaluator implements ExpressionEvaluator {
    private final AggregationFactory aggregations;

    @Inject
    public CoreExpressionEvaluator(final AggregationFactory aggregations) {
        this.aggregations = aggregations;
    }

    @Override
    public QueryInstance evaluateQuery(final Expression q) {
        return q.visit(new Expression.Visitor<QueryInstance>() {
            @Override
            public QueryInstance visitQuery(final QueryExpression e) {
                final Optional<Aggregation> aggregation =
                    e.getSelect().flatMap(aggregations::fromExpression);

                /* get aggregation that is part of statement, if any */
                final Optional<Function<Expression.Scope, DateRange>> range =
                    e.getRange().map(r -> scope -> {
                        final RangeExpression expanded = r.eval(scope);
                        final long start = expanded.getStart().cast(Long.class);
                        final long end = expanded.getEnd().cast(Long.class);
                        return new DateRange(start, end);
                    });

                return new QueryInstance(Optional.empty(), ImmutableMap.of(), ImmutableSet.of(),
                    e.getSource(), e.getFilter(), aggregation, Optional.empty(), Optional.empty(),
                    range, Function.identity(), e.getModifiers());
            }
        });
    }
}
