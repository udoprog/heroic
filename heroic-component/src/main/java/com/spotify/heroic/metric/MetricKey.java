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

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.common.Series;
import lombok.Data;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

@Data
public class MetricKey {
    static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    private final Optional<String> key;
    private final SortedMap<String, String> tags;

    public HashCode hash() {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        key.ifPresent(key -> {
            hasher.putString(key, Charsets.UTF_8);
        });

        for (final Map.Entry<String, String> e : tags.entrySet()) {
            hasher.putString(e.getKey(), Charsets.UTF_8);
            hasher.putString(e.getValue(), Charsets.UTF_8);
        }

        return hasher.hash();
    }

    public static MetricKey of(final Series series) {
        return new MetricKey(Optional.ofNullable(series.getKey()), series.getTags());
    }
}
