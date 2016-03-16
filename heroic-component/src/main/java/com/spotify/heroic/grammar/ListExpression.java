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
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@ValueName("list")
@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("list")
@RequiredArgsConstructor
public final class ListExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final List<? extends Expression> list;

    @JsonCreator
    public ListExpression(
        @JsonProperty("list") final List<? extends Expression> list
    ) {
        this(Context.empty(), list);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitList(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public Expression add(Expression other) {
        final ListExpression o = other.cast(this);
        final ArrayList<Expression> list = new ArrayList<Expression>();
        list.addAll(this.list);
        list.addAll(o.list);
        return new ListExpression(ctx.join(other.context()), list);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof ListExpression) {
            return (T) this;
        }

        throw ctx.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(ListExpression.class)) {
            return (T) this;
        }

        throw ctx.castError(this, to);
    }

    @Override
    public String toString() {
        return list.toString();
    }
}
