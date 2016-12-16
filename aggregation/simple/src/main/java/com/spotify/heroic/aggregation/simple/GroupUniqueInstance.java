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

import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.CompositeCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import lombok.ToString;

import java.beans.ConstructorProperties;
import java.util.List;

@ToString(callSuper = true)
public class GroupUniqueInstance extends BucketAggregationInstance<GroupUniqueBucket> {
    @ConstructorProperties({"size", "extent"})
    public GroupUniqueInstance(final long size, final long extent) {
        super(size, extent, BucketAggregationInstance.ALL_TYPES, MetricType.GROUP);
    }

    @Override
    protected GroupUniqueBucket buildBucket(long timestamp) {
        return new GroupUniqueBucket(timestamp);
    }

    @Override
    protected Metric build(final GroupUniqueBucket bucket) {
        final List<CompositeCollection> groups = bucket.groups();

        if (groups.isEmpty()) {
            return Metric.invalid();
        }

        return new MetricGroup(bucket.timestamp(), groups);
    }
}
