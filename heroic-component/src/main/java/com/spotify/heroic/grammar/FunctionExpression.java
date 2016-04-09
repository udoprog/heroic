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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationArguments;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;
import java.util.Map;

@ValueName("function")
@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("function")
@RequiredArgsConstructor
public class FunctionExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final String name;
    private final ListExpression arguments;
    private final Map<String, Expression> keywordArguments;

    @JsonCreator
    public FunctionExpression(
        @JsonProperty("name") final String name,
        @JsonProperty("arguments") final ListExpression arguments,
        @JsonProperty("keywordArguments") final Map<String, Expression> keywordArguments
    ) {
        this(Context.empty(), name, arguments, keywordArguments);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitFunction(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof FunctionExpression) {
            return (T) this;
        }

        throw ctx.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(FunctionExpression.class)) {
            return (T) this;
        }

        throw ctx.castError(this, to);
    }

    @Override
    public String toString() {
        final Joiner args = Joiner.on(", ");
        final Iterator<String> a =
            this.arguments.getList().stream().map(v -> v.toString()).iterator();
        final Iterator<String> k = keywordArguments
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .iterator();
        return "" + name + "(" + args.join(Iterators.concat(a, k)) + ")";
    }

    public AggregationArguments arguments() {
        return new AggregationArguments(getArguments().getList(), getKeywordArguments());
    }
}
