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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import lombok.Data;

import java.util.Map;

@Data
public class AggregationState {
    final Map<String, String> key;
    final Iterable<Series> series;
    final Observable<MetricCollection> observable;

    /**
     * Create a state representing a single series.
     *
     * @param s The seres it represents.
     * @param observable Observable providing data for this series.
     * @return An aggregation state representing a single series.
     */
    public static AggregationState forSeries(
        final Series s, final Observable<MetricCollection> observable
    ) {
        return new AggregationState(s.getTags(), ImmutableList.of(s), observable);
    }

    public AggregationState withKey(final Map<String, String> key) {
        return new AggregationState(key, series, observable);
    }

    public AggregationState withObservable(final Observable<MetricCollection> observable) {
        return new AggregationState(key, series, observable);
    }
}
