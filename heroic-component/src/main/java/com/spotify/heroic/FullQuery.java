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

package com.spotify.heroic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class FullQuery {
    private final MetricType source;
    private final Filter filter;
    private final DateRange range;
    private final Aggregation aggregation;
    private final QueryOptions options;
    private final Duration size;
    private final Set<String> features;
    private final long now;
    private final Map<String, String> as;

    @JsonCreator
    public FullQuery(
        @JsonProperty("source") final MetricType source,
        @JsonProperty("filter") final Filter filter, @JsonProperty("range") final DateRange range,
        @JsonProperty("aggregation") final Aggregation aggregation,
        @JsonProperty("options") final QueryOptions options,
        @JsonProperty("size") final Duration size,
        @JsonProperty("features") final Set<String> features, @JsonProperty("now") final Long now,
        @JsonProperty("as") final Map<String, String> as
    ) {
        this.source = source;
        this.filter = filter;
        this.range = range;
        this.aggregation = aggregation;
        this.options = options;
        this.size = size;
        this.features = features;
        this.now = now;
        this.as = as;
    }
}
