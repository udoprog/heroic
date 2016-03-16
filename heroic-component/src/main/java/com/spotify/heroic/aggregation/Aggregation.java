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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Duration;
import eu.toolchain.async.AsyncFuture;

import java.util.Optional;
import java.util.Set;

/**
 * A metric manipulation framework.
 * <p>
 * Aggregations are responsible for shaping metrics into a more desirable format. They can perform
 * down-sampling, e.g. {@link com.spotify.heroic.aggregation.BucketAggregation} or filter data.
 * <p>
 * All members must be fully thread-safe. In order to perform one you would use the {@link
 * #setup(com.spotify.heroic.aggregation.AggregationContext)}} and read data from the returned
 * {@link com.spotify.heroic.async.Observable} instances. The returned Observable's are not
 * sequential and can be called from multiple threads.
 *
 * @author udoprog
 */
public interface Aggregation {
    /**
     * Indicate at which interval the current aggregation will output data.
     *
     * @return An optional with the interval, or empty if it cannot be determined.
     */
    default Optional<Duration> cadence() {
        return Optional.empty();
    }

    /**
     * Get a set of required tags.
     * <p>
     * This is used to elide a set of required tags that needs to be forwarded for a certain
     * aggregation.
     *
     * @return Return the set of required tags for this aggregation.
     */
    default Set<String> requiredTags() {
        return ImmutableSet.of();
    }

    /**
     * Convert the aggregation into the corresponding distributed aggregation.
     * <p>
     * The distributed aggregation is the aggregation that will be sent over the wire.
     *
     * @return The distributed aggregation.
     */
    default Aggregation distributed() {
        return this;
    }

    default Aggregation combiner() {
        return Empty.INSTANCE;
    }

    /**
     * @return {@code true} if this aggregation references another expression. {@code false}
     * otherwise.
     */
    boolean referential();

    /**
     * Traverse the possible aggregations and build the necessary graph out of them.
     */
    AsyncFuture<AggregationContext> setup(AggregationContext context);
}
