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

package com.spotify.heroic;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Data
public class QueryInstance {
    private final Map<String, QueryInstance> statements;
    private final MetricType source;
    private final Filter filter;
    private final Optional<DateRange> range;
    private final Aggregation aggregation;
    private final QueryOptions options;
    private final Optional<Duration> cadence;
    private final Set<String> features;

    public boolean hasFeature(final String feature) {
        return features.contains(feature);
    }

    public QueryInstance withAggregation(final Aggregation aggregation) {
        return new QueryInstance(statements, source, filter, range, aggregation, options, cadence,
            features);
    }

    public QueryInstance andFilter(final FilterFactory filters, final Filter f) {
        return new QueryInstance(statements, source, filters.and(filter, f), range, aggregation,
            options, cadence, features);
    }

    public QueryInstance withParentStatements(final Map<String, QueryInstance> statements) {
        final ImmutableMap<String, QueryInstance> s = ImmutableMap.<String, QueryInstance>builder()
            .putAll(statements)
            .putAll(this.statements)
            .build();

        return new QueryInstance(s, source, filter, range, aggregation, options, cadence, features);
    }

    public QueryInstance withRangeIfAbsent(final Optional<DateRange> other) {
        final Optional<DateRange> range = Optionals.firstPresent(this.range, other);

        return new QueryInstance(statements, source, filter, range, aggregation, options, cadence,
            features);
    }
}
