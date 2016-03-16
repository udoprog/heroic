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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.MetricCollector;
import com.spotify.heroic.aggregation.MetricsCollector;

public class EmptyMetricCollection extends MetricCollection {
    public EmptyMetricCollection() {
        super(MetricType.POINT, ImmutableList.of());
    }

    @Override
    public void update(
        MetricsCollector session
    ) {
    }

    @Override
    public void update(MetricCollector collector) {
    }

    @Override
    public MetricCollection apply(
        final MetricCollectionBiFunction function, final MetricCollection right
    ) {
        return this;
    }

    @Override
    public MetricCollection apply(final MetricCollectionFunction function) {
        return this;
    }
}
