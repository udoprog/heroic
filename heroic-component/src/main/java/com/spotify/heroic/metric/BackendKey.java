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

package com.spotify.heroic.metric;

import com.spotify.heroic.common.Series;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.SortedMap;

@Data
@RequiredArgsConstructor
public class BackendKey {
    private final Optional<String> key;
    private final SortedMap<String, String> tags;
    private final long base;
    private final MetricType type;
    private final Optional<Long> token;

    public static BackendKey of(final Series series, final long base) {
        return new BackendKey(series.getKey(), series.getTags(), base, MetricType.POINT,
            Optional.empty());
    }

    public static BackendKey of(
        final Optional<String> key, final SortedMap<String, String> tags, final long base
    ) {
        return new BackendKey(key, tags, base, MetricType.POINT, Optional.empty());
    }

    public MetricKey toMetricKey() {
        return new MetricKey(key, tags);
    }
}
