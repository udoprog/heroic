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
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleBinaryOperator;
import lombok.RequiredArgsConstructor;

/**
 * Bucket that keeps track of the amount of data points seen, and there summed value.
 */
@RequiredArgsConstructor
public class SpreadBucket extends AbstractBucket {
    static final DoubleBinaryOperator MIN_FN = Math::min;
    static final DoubleBinaryOperator MAX_FN = Math::max;

    final long timestamp;

    final LongAdder count = new LongAdder();
    final DoubleAdder sum = new DoubleAdder();
    final DoubleAdder sum2 = new DoubleAdder();
    final DoubleAccumulator max = new DoubleAccumulator(MAX_FN, Double.NEGATIVE_INFINITY);
    final DoubleAccumulator min = new DoubleAccumulator(MIN_FN, Double.POSITIVE_INFINITY);

    @Override
    public void updateSpread(Map<String, String> key, Spread d) {
        count.add(d.getCount());
        sum.add(d.getSum());
        sum2.add(d.getSum2());
        max.accumulate(d.getMax());
        min.accumulate(d.getMin());
    }

    @Override
    public void updatePoint(Map<String, String> key, Point d) {
        final double value = d.getValue();

        if (!Double.isFinite(value)) {
            return;
        }

        count.increment();
        sum.add(value);
        sum2.add(value * value);
        max.accumulate(value);
        min.accumulate(value);
    }

    /**
     * Convert into a spread.
     *
     * @return the spread, or {@code null} if there are no samples.
     */
    Metric asSpread() {
        final long count = this.count.sum();

        if (count == 0) {
            return null;
        }

        return new Spread(timestamp, count, sum.sum(), sum2.sum(), min.get(), max.get());
    }
}
