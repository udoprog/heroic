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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Aggregations;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Data
@RequiredArgsConstructor
public class Query {
    public static final String DISTRIBUTED_AGGREGATIONS =
        "com.spotify.heroic.distributed_aggregations";

    private final Map<String, Query> statements;
    private final Optional<Aggregation> aggregation;
    private final Optional<MetricType> source;
    private final Optional<QueryDateRange> range;
    private final Optional<Filter> filter;
    private final QueryOptions options;
    private final Optional<List<String>> groupBy;
    /* set of experimental features to enable */
    private final Set<String> features;

    @JsonCreator
    public Query(
        @JsonProperty("statements") final Optional<Map<String, Query>> statements,
        @JsonProperty("aggregators") final Optional<List<Aggregation>> aggregators,
        @JsonProperty("aggregation") final Optional<Aggregation> aggregation,
        @JsonProperty("source") final Optional<MetricType> source,
        @JsonProperty("range") final Optional<QueryDateRange> range,
        @JsonProperty("filter") final Optional<Filter> filter,
        @JsonProperty("options") final Optional<QueryOptions> options,
        @JsonProperty("groupBy") final Optional<List<String>> groupBy,
        @JsonProperty("features") final Optional<Set<String>> features
    ) {
        this.statements = statements.orElseGet(ImmutableMap::of);
        this.filter = filter;
        this.range = range;
        this.aggregation =
            Optionals.pickOptional(aggregation, aggregators.flatMap(Aggregations::chain));
        this.source = source;
        this.options = options.orElseGet(QueryOptions::defaults);
        this.groupBy = groupBy;
        this.features = features.orElseGet(ImmutableSet::of);
    }

    public Query withOptions(QueryOptions options) {
        return new Query(statements, aggregation, source, range, filter, options, groupBy,
            features);
    }

    public Optional<Aggregation> getAggregation() {
        return aggregation;
    }

    /**
     * Check if a specific experimental feature is implemented for this query.
     *
     * @param feature Feature to check for.
     * @return {@code true} if the feature is enabled.
     */
    public boolean hasFeature(final String feature) {
        return features.contains(feature);
    }
}
