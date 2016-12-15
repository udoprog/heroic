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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface BaseCollection {
    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    <T> Iterable<T> dataAs(Class<T> expected);

    default <T> Stream<T> streamAs(Class<T> expected) {
        return StreamSupport.stream(dataAs(expected).spliterator(), false);
    }

    /**
     * Update the given aggregation with the content of this collection.
     */
    void updateAggregation(
        AggregationSession session, Map<String, String> tags, Set<Series> series
    );

    long size();

    MetricType type();

    @JsonIgnore
    default boolean isEmpty() {
        return size() == 0;
    }
}
