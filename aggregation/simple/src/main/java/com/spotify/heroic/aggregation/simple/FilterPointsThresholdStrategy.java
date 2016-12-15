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

import com.spotify.heroic.metric.CompositeCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class FilterPointsThresholdStrategy implements MetricMappingStrategy {
    private final FilterKThresholdType filterType;
    private final double threshold;

    @Override
    public CompositeCollection apply(CompositeCollection metrics) {
        if (metrics.type() == MetricType.POINT) {
            return CompositeCollection.build(MetricType.POINT,
                filterWithThreshold(metrics.dataAs(Point.class)));
        } else {
            return metrics;
        }
    }

    private List<Point> filterWithThreshold(Iterable<Point> points) {
        final List<Point> result = new ArrayList<>();

        for (final Point p : points) {
            if (filterType.predicate(p.getValue(), threshold)) {
                result.add(p);
            }
        }

        return result;
    }
}
