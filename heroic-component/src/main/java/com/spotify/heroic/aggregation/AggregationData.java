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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public final class AggregationData {
    private final Map<String, String> key;
    private final Iterable<Series> series;
    private final MetricCollection metrics;
    private final DateRange range;

    @JsonCreator
    public AggregationData(
        @JsonProperty("key") final Map<String, String> key,
        @JsonProperty("series") final Iterable<Series> series,
        @JsonProperty("metrics") final MetricCollection metrics,
        @JsonProperty("range") final DateRange range
    ) {
        this.key = key;
        this.series = series;
        this.metrics = metrics;
        this.range = range;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return metrics.isEmpty();
    }

    @JsonProperty("series")
    public List<Series> getSeriesJson() {
        return ImmutableList.copyOf(series);
    }

    public AggregationData withAs(final Map<String, String> as) {
        final Map<String, String> modified = new HashMap<>();
        modified.putAll(key);
        modified.putAll(as);
        return new AggregationData(modified, series, metrics, range);
    }
}
