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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationScope;
import com.spotify.heroic.aggregation.CompoundAggregation;
import com.spotify.heroic.aggregation.CompoundAggregationInstance;
import com.spotify.heroic.grammar.Expression;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

public class Plus implements CompoundAggregation {
    private final Expression left;
    private final Expression right;

    public Plus(
        @JsonProperty("left") final Expression left, @JsonProperty("right") final Expression right
    ) {
        this.left = left;
        this.right = right;
    }

    @Override
    public CompoundAggregationInstance apply(final AggregationContext aggregationContext) {
        return new PlusInstance(left, right);
    }

    @RequiredArgsConstructor
    public static class PlusInstance implements CompoundAggregationInstance {
        private final Expression left;
        private final Expression right;

        @Override
        public AsyncFuture<AggregationResult> calculate(final AggregationScope scope) {
            final AsyncFuture<AggregationResult> left = scope.runQuery(this.left);
            final AsyncFuture<AggregationResult> right = scope.runQuery(this.right);

            return left;
        }
    }
}
