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
import com.spotify.heroic.metric.CompositeCollection;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public final class AggregationOutput {
    private final Map<String, String> key;
    private final Set<Series> series;
    private final CompositeCollection metrics;

    public boolean isEmpty() {
        return metrics.isEmpty();
    }

    public AggregationOutput withKey(final Map<String, String> key) {
        return new AggregationOutput(key, series, metrics);
    }
}
