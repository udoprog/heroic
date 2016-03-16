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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Data
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class QueryTrace {
    private final Identifier what;
    private final long elapsed;
    private final List<QueryTrace> children;

    public QueryTrace(
        final QueryTrace.Identifier what, final Stopwatch w, final List<QueryTrace> children
    ) {
        this(what, w.elapsed(TimeUnit.MICROSECONDS), children);
    }

    public QueryTrace(final QueryTrace.Identifier what) {
        this(what, 0L, ImmutableList.of());
    }

    public QueryTrace(final QueryTrace.Identifier what, final Stopwatch w) {
        this(what, w, ImmutableList.of());
    }

    public QueryTrace(final QueryTrace.Identifier what, final long elapsedMicros) {
        this(what, elapsedMicros, ImmutableList.of());
    }

    @JsonCreator
    public static QueryTrace jsonCreate(
        @JsonProperty("what") final QueryTrace.Identifier what,
        @JsonProperty("elapsed") final Optional<Long> elapsed,
        @JsonProperty("children") final List<QueryTrace> children
    ) {
        return new QueryTrace(what,
            elapsed.orElseThrow(() -> new IllegalArgumentException("elapsed")), children);
    }

    public void formatTrace(PrintWriter out) {
        formatTrace("", out);
    }

    public void formatTrace(String prefix, PrintWriter out) {
        out.println(prefix + what + " (in " + readableTime(elapsed) + ")");

        for (final QueryTrace child : children) {
            child.formatTrace(prefix + "  ", out);
        }
    }

    public static Identifier identifier(Class<?> where) {
        return new Identifier(where.getName());
    }

    public static Identifier identifier(Class<?> where, String what) {
        return new Identifier(where.getName() + "#" + what);
    }

    public static Identifier identifier(String description) {
        return new Identifier(description);
    }

    public static Tracer trace(final Identifier identifier) {
        return new Tracer(identifier, Stopwatch.createStarted());
    }

    private String readableTime(long elapsed) {
        if (elapsed > 1000000000) {
            return String.format("%.3fs", elapsed / 1000000000d);
        }

        if (elapsed > 1000000) {
            return String.format("%.3fms", elapsed / 1000000d);
        }

        if (elapsed > 1000) {
            return String.format("%.3fus", elapsed / 1000d);
        }

        return elapsed + "ns";
    }

    @Data
    public static class Identifier {
        private final String name;

        @JsonCreator
        public Identifier(@JsonProperty("name") String name) {
            this.name = name;
        }

        public Identifier extend(String key) {
            return new Identifier(name + "[" + key + "]");
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Tracer {
        private final Identifier identifier;
        private final Stopwatch w;

        public QueryTrace end() {
            return end(ImmutableList.of());
        }

        public QueryTrace end(QueryTrace child) {
            return end(ImmutableList.of(child));
        }

        public QueryTrace end(List<QueryTrace> children) {
            return new QueryTrace(identifier, w.elapsed(TimeUnit.MICROSECONDS), children);
        }
    }
}
