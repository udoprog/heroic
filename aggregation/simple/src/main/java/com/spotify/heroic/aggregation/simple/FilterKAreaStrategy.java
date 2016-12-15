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
import com.spotify.heroic.metric.Point;
import lombok.Data;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This filter strategy calculates the area under the graphs of the time series and
 * selects the time series with either the biggest (TopK) or smallest (BottomK) area.
 * <p>
 * Time series without any data points are disregarded and never part of the result.
 */
@Data
public class FilterKAreaStrategy implements FilterStrategy {
    private final FilterKAreaType filterType;
    private final long k;

    @Override
    public <T> List<T> filter(List<FilterableData<T>> metrics) {
        return metrics
            .stream()
            .filter(m -> !m.getMetrics().isEmpty())
            .map(m -> {
                final double area = computeArea(m.getMetrics());
                return new AreaData<>(m.getData(), area);
            })
            .sorted((a, b) -> filterType.compare(a.getArea(), b.getArea()))
            .limit(k)
            .map(AreaData::getData)
            .collect(Collectors.toList());
    }

    private double computeArea(final CompositeCollection metrics) {
        final Iterator<Point> it = metrics.dataAs(Point.class).iterator();

        double area = 0;

        if (it.hasNext()) {
            Point previous = it.next();

            while (it.hasNext()) {
                Point next = it.next();
                area += PointPairArea.computeArea(previous, next);
                previous = next;
            }
        }

        return area;
    }

    @Data
    private static class AreaData<T> {
        private final T data;
        private final double area;
    }
}
