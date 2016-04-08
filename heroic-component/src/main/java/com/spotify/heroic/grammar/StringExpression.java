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
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ValueName("string")
@Data
@EqualsAndHashCode(exclude = {"ctx"})
@RequiredArgsConstructor
public final class StringExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final String string;

    @JsonCreator
    public StringExpression(@JsonProperty("string") final String string) {
        this(Context.empty(), string);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitString(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public Expression add(Expression other) {
        final StringExpression o = other.cast(this);
        return new StringExpression(ctx.join(o.ctx), string + o.string);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof StringExpression) {
            return (T) this;
        }

        if (to instanceof Long) {
            try {
                return (T) Long.valueOf(string);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("not a valid int");
            }
        }

        throw ctx.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(StringExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(ListExpression.class)) {
            return (T) Expression.list(this);
        }

        if (to.isAssignableFrom(FunctionExpression.class)) {
            return (T) new FunctionExpression(ctx, string, Expression.list(), ImmutableMap.of());
        }

        if (to == String.class) {
            return (T) string;
        }

        throw ctx.castError(this, to);
    }

    @Override
    public String toString() {
        return QueryParser.escapeString(string);
    }
}
