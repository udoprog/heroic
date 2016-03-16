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
import com.spotify.heroic.grammar.Expression;

import java.util.Optional;
import java.util.function.Function;

public abstract class FilterAggregationBuilder<T extends Aggregation, K>
    implements Function<AggregationArguments, Aggregation> {
    private final Class<K> kType;

    public FilterAggregationBuilder(Class<K> kType) {
        this.kType = kType;
    }

    @Override
    public Aggregation apply(AggregationArguments args) {
        final K k = args
            .getNext("k", kType)
            .orElseThrow(() -> new IllegalArgumentException("missing required 'k' argument"));

        final Optional<Expression> reference = args.getNext("reference", Expression.class);
        return build(args, k, reference);
    }

    protected abstract T build(
        AggregationArguments args, K k, Optional<Expression> reference
    );
}
