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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Group extends GroupingAggregation {
    public static final String NAME = "group";

    public static final Map<String, String> ALL_GROUP = ImmutableMap.of();

    public Group(
        Optional<List<String>> of, Optional<Aggregation> each, Optional<Expression> reference
    ) {
        super(of, each, reference);
    }

    @JsonCreator
    public Group(
        @JsonProperty("of") Optional<List<String>> of, @JsonProperty("each") AggregationOrList each,
        @JsonProperty("reference") Optional<Expression> reference
    ) {
        this(of, each.toAggregation(), reference);
    }

    public Group(Optional<List<String>> of, Optional<Aggregation> each) {
        this(of, new AggregationOrList(each), Optional.empty());
    }

    @Override
    protected Map<String, String> key(
        final Map<String, String> tags, final Optional<Set<String>> of
    ) {
        return of.map(o -> {
            // group by 'everything'
            if (o.isEmpty()) {
                return ALL_GROUP;
            }

            final Map<String, String> key = new HashMap<>();

            for (final String tag : o) {
                String value = tags.get(tag);

                if (value == null) {
                    continue;
                }

                key.put(tag, value);
            }

            return key;
        }).orElse(tags);
    }
}
