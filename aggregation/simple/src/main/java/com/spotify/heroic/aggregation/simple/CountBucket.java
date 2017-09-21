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

import com.spotify.heroic.aggregation.AnyBucket;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.Point;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;

/**
 * Bucket that counts the number of seen samples.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class CountBucket implements AnyBucket, PointBucket {
    private final long timestamp;

    private final AtomicLong count = new AtomicLong();

    @Override
    public void update(Map<String, String> key, Metric d) {
        count.incrementAndGet();
    }

    @Override
    public Point asPoint() {
        return new Point(timestamp, count.get());
    }
}
