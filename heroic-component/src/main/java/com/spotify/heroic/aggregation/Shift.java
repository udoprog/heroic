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
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.DurationExpression;
import com.spotify.heroic.grammar.Expression;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Data
public class Shift implements Aggregation {
    public static final String NAME = "shift";

    private final Optional<Expression> reference;
    private final Expression amount;

    private static final Expression.Visitor<Duration> TO_DURATION =
        new Expression.Visitor<Duration>() {
            @Override
            public Duration visitDuration(final DurationExpression e) {
                return Duration.of(e.getValue(), e.getUnit());
            }
        };

    @JsonCreator
    public Shift(
        @JsonProperty("reference") final Optional<Expression> reference,
        @JsonProperty("amount") final Optional<Expression> amount
    ) {
        this.reference = reference;
        this.amount = amount.orElseGet(() -> Expression.duration(TimeUnit.MILLISECONDS, 0));
    }

    @Override
    public boolean referential() {
        return reference.isPresent();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext context) {
        final Duration shift = context.eval(amount).visit(TO_DURATION);
        final DateRange shifted = context.range().shift(shift.toMilliseconds());
        final LookupOverrides overrides = new LookupOverrides(Optional.of(
            Expression.range(Expression.integer(shifted.start()),
                Expression.integer(shifted.end()))));

        return context.lookupContext(reference).apply(overrides);
    }
}
