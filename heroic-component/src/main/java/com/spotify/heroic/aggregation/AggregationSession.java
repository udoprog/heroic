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

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AggregationSession {
    default void updatePoints(
        Map<String, String> key, Set<Series> series, List<Point> values
    ) {
        updatePoints(key, series, values, values.size());
    }

    void updatePoints(
        Map<String, String> key, Set<Series> series, Iterable<Point> values, long size
    );

    default void updateEvents(
        Map<String, String> key, Set<Series> series, List<Event> values
    ) {
        updateEvents(key, series, values, values.size());
    }

    void updateEvents(
        Map<String, String> key, Set<Series> series, Iterable<Event> values, long size
    );

    default void updateSpreads(
        Map<String, String> key, Set<Series> series, List<Spread> values
    ) {
        updateSpreads(key, series, values, values.size());
    }

    void updateSpreads(
        Map<String, String> key, Set<Series> series, Iterable<Spread> values, long size
    );

    default void updateGroup(
        Map<String, String> key, Set<Series> series, List<MetricGroup> values
    ) {
        updateGroup(key, series, values, values.size());
    }

    void updateGroup(
        Map<String, String> key, Set<Series> series, Iterable<MetricGroup> values, long size
    );

    default void updatePayload(
        Map<String, String> key, Set<Series> series, List<Payload> values
    ) {
        updatePayload(key, series, values, values.size());
    }

    void updatePayload(
        Map<String, String> key, Set<Series> series, Iterable<Payload> values, long size
    );

    /**
     * Get the result of this aggregator.
     */
    AggregationResult result();
}
