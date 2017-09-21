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

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.function.DoubleBinaryOperator;
import lombok.RequiredArgsConstructor;

/**
 * A min-bucket implementation intended to reduce cross-thread contention.
 * <p>
 * This bucket uses primitives based on striped atomic updates to reduce contention across CPUs.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class StripedMinBucket extends AbstractBucket implements PointBucket {
    private static final DoubleBinaryOperator minFn = Math::min;

    private final long timestamp;

    private final DoubleAccumulator min = new DoubleAccumulator(minFn, Double.POSITIVE_INFINITY);

    @Override
    public void updateSpread(Map<String, String> key, Spread d) {
        min.accumulate(d.getMin());
    }

    @Override
    public void updatePoint(Map<String, String> key, Point d) {
        min.accumulate(d.getValue());
    }

    @Override
    public Point asPoint() {
        final double result = min.doubleValue();

        if (!Double.isFinite(result)) {
            return null;
        }

        return new Point(timestamp, result);
    }
}
