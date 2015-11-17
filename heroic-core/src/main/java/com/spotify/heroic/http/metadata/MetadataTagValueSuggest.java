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

package com.spotify.heroic.http.metadata;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.suggest.MatchOptions;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.NONE)
public class MetadataTagValueSuggest {
    private static final int DEFAULT_LIMIT = 10;
    private static final String DEFAULT_VALUE = "";

    /**
     * Filter the suggestions being returned.
     */
    private final Optional<Filter> filter;

    /**
     * Limit the number of suggestions being returned.
     */
    private final int limit;

    /**
     * Query for tags within the given range.
     */
    private final Optional<QueryDateRange> range;

    private final MatchOptions match;

    /**
     * Exclude the given tags from the result.
     */
    private final String key;

    /**
     * Basis for suggestion.
     */
    private final String value;

    @JsonCreator
    public MetadataTagValueSuggest(@JsonProperty("filter") Optional<Filter> filter,
            @JsonProperty("range") Optional<QueryDateRange> range,
            @JsonProperty("limit") Optional<Integer> limit, @JsonProperty("match") MatchOptions match, @JsonProperty("key") String key, @JsonProperty("value") String value) {
        this.filter = filter;
        this.range = range;
        this.limit = limit.orElse(DEFAULT_LIMIT);
        this.match = Optional.ofNullable(match).orElseGet(MatchOptions.builder()::build);
        this.key = Preconditions.checkNotNull(key, "key must not be null");
        this.value = Optional.ofNullable(value).orElse(DEFAULT_VALUE);
    }

    public static MetadataTagValueSuggest createDefault() {
        return new MetadataTagValueSuggest(Optional.empty(), Optional.empty(), Optional.empty(), null,
                "", null);
    }
}
