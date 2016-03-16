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

package com.spotify.heroic.grammar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("query")
@RequiredArgsConstructor
public class QueryExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final Optional<Expression> select;
    private final Optional<MetricType> source;
    private final Optional<RangeExpression> range;
    private final Optional<Filter> filter;

    @JsonCreator
    public QueryExpression(
        @JsonProperty("select") final Optional<Expression> select,
        @JsonProperty("source") final Optional<MetricType> source,
        @JsonProperty("range") final Optional<RangeExpression> range,
        @JsonProperty("filter") final Optional<Filter> filter
    ) {
        this(Context.empty(), select, source, range, filter);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitQuery(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public String toString() {
        return String.format("{select: %s source: %s, range: %s, filter: %s}", select, source,
            range, filter);
    }
}
