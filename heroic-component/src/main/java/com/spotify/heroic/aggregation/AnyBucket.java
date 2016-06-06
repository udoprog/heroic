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

import com.spotify.heroic.metric.Cardinality;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import java.util.Map;

public interface AnyBucket extends Bucket {
    @Override
    default void updatePoint(Map<String, String> tags, Point sample) {
        update(tags, sample);
    }

    @Override
    default void updateEvent(Map<String, String> tags, Event sample) {
        update(tags, sample);
    }

    @Override
    default void updateSpread(Map<String, String> tags, Spread sample) {
        update(tags, sample);
    }

    @Override
    default void updateGroup(Map<String, String> tags, MetricGroup sample) {
        update(tags, sample);
    }

    @Override
    default void updateCardinality(Map<String, String> tags, Cardinality sample) {
        update(tags, sample);
    }

    void update(Map<String, String> tags, Metric sample);
}
