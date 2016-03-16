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

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationArguments;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.Expression;

import java.util.Optional;
import java.util.function.Function;

public abstract class SamplingAggregationDSL<T>
    implements Function<AggregationArguments, Aggregation> {
    @Override
    public Aggregation apply(final AggregationArguments args) {
        final Optional<Expression> reference = args.getNext("reference", Expression.class);
        final Optional<Duration> size = args.getNext("size", Duration.class);
        final Optional<Duration> extent = args.getNext("extent", Duration.class);

        return buildWith(args, size, extent, reference);
    }

    protected abstract Aggregation buildWith(
        AggregationArguments args, Optional<Duration> size, Optional<Duration> extent,
        Optional<Expression> reference
    );
}
