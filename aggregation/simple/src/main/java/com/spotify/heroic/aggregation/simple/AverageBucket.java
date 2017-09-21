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

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AverageBucket extends AbstractBucket implements PointBucket {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble();
    private final AtomicLong count = new AtomicLong();

    @Override
    public void updatePoint(Map<String, String> key, Point d) {
        value.addAndGet(d.getValue());
        count.incrementAndGet();
    }

    @Override
    public void updateSpread(Map<String, String> key, Spread sample) {
        value.addAndGet(sample.getSum());
        count.addAndGet(sample.getCount());
    }

    @Override
    public Point asPoint() {
        final long count = this.count.get();

        if (count == 0) {
            return null;
        }

        return new Point(timestamp, value.get() / count);
    }
}
