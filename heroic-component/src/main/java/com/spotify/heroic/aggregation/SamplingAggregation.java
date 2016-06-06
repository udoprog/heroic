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
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.common.Duration;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.common.Optionals.firstPresent;

@Data
public abstract class SamplingAggregation implements Aggregation {
    public static final Joiner params = Joiner.on(", ");

    private final Optional<Duration> size;
    private final Optional<Duration> extent;

    @Override
    public AggregationInstance apply(final AggregationContext context) {
        final Duration s = firstPresent(size, context.size()).orElseGet(context::defaultSize);
        final Duration e = firstPresent(firstPresent(extent, size), context.extent()).orElse(s);
        return apply(context, s.convert(TimeUnit.MILLISECONDS), e.convert(TimeUnit.MILLISECONDS));
    }

    @Override
    public Optional<Long> size() {
        return size.map(Duration::toMilliseconds);
    }

    @Override
    public Optional<Long> extent() {
        return extent.map(Duration::toMilliseconds);
    }

    protected abstract AggregationInstance apply(
        AggregationContext context, long size, long extent
    );
}
