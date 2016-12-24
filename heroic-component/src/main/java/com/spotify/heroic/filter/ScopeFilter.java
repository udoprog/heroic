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

package com.spotify.heroic.filter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.DSL;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.StringJoiner;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "scope"}, doNotUseGetters = true)
public class ScopeFilter implements Filter {
    public static final String OPERATOR = "scope";

    private final Series.Scope scope;

    public ScopeFilter(final Iterable<Map.Entry<String, String>> filters) {
        this.scope = new Series.ListScope(ImmutableList.copyOf(filters));
    }

    @Override
    public boolean apply(Series series) {
        return series.getScope().equals(scope);
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitScope(this);
    }

    @Override
    public Filter optimize() {
        return this;
    }

    @Override
    public String toString() {
        final StringJoiner joiner = new StringJoiner(", ", "", "");

        for (final Map.Entry<String, String> entry : scope) {
            joiner.add(entry.toString());
        }

        return "scope(" + joiner.toString() + ")";
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (!ScopeFilter.class.equals(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final ScopeFilter other = (ScopeFilter) o;
        return scope.compareTo(other.scope);
    }

    private static final Joiner COMMA = Joiner.on(", ");

    @Override
    public String toDSL() {
        return "$scope(" + COMMA.join(Iterators.transform(scope.iterator(),
            m -> DSL.dumpString(m.getKey()) + ":" + DSL.dumpString(m.getValue()))) + ")";
    }
}
