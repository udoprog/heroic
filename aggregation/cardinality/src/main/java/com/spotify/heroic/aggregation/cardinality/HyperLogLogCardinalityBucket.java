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

package com.spotify.heroic.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.metric.Cardinality;
import com.spotify.heroic.metric.Metric;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
public class HyperLogLogCardinalityBucket implements CardinalityBucket {
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(128);

    private final long timestamp;

    private final HyperLogLog seen;
    private final ConcurrentLinkedQueue<HyperLogLog> states = new ConcurrentLinkedQueue<>();

    public HyperLogLogCardinalityBucket(final long timestamp, final double std) {
        this.timestamp = timestamp;
        this.seen = new HyperLogLog(std);
    }

    public long timestamp() {
        return timestamp;
    }

    private static final Ordering<String> KEY_ORDER = Ordering.from(String::compareTo);

    @Override
    public void updateCardinality(
        final Map<String, String> tags, final Cardinality sample
    ) {
        try {
            states.add(HyperLogLog.Builder.build(sample.getState()));
        } catch (final IOException e) {
            throw new RuntimeException("Failed to deserialize state", e);
        }
    }

    @Override
    public void update(final Map<String, String> tags, final Metric d) {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        for (final String k : KEY_ORDER.sortedCopy(tags.keySet())) {
            hasher.putString(k, Charsets.UTF_8).putString(tags.get(k), Charsets.UTF_8);
        }

        d.hash(hasher);
        seen.offerHashed(hasher.hash().asLong());
    }

    @Override
    public long count() {
        return seen.cardinality();
    }

    public byte[] state() {
        try {
            return seen.getBytes();
        } catch (final IOException e) {
            throw new RuntimeException("Could not persist state", e);
        }
    }
}
