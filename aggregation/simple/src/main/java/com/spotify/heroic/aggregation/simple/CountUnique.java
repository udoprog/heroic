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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Optional;

@Data
@EqualsAndHashCode(callSuper = true)
public class CountUnique extends BucketAggregation<CountUniqueBucket> {
    public static final String NAME = "count-unique";

    @JsonCreator
    public CountUnique(
        @JsonProperty("sampling") final Optional<SamplingQuery> sampling,
        @JsonProperty("size") final Optional<Duration> size,
        @JsonProperty("extent") final Optional<Duration> extent,
        @JsonProperty("reference") final Optional<Expression> reference
    ) {
        super(Optionals.firstPresent(size, sampling.flatMap(SamplingQuery::getSize)),
            Optionals.firstPresent(extent, sampling.flatMap(SamplingQuery::getExtent)), reference,
            ALL_TYPES, MetricType.POINT);
    }

    @Override
    protected CountUniqueBucket buildBucket(long timestamp) {
        return new CountUniqueBucket(timestamp);
    }

    @Override
    protected Point build(CountUniqueBucket bucket) {
        return new Point(bucket.timestamp(), bucket.count());
    }
}
