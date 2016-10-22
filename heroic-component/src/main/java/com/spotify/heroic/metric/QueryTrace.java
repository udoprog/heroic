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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type",
    defaultImpl = QueryTrace.ActiveTrace.class)
@JsonSubTypes({
    @JsonSubTypes.Type(QueryTrace.ActiveTrace.class),
    @JsonSubTypes.Type(QueryTrace.PassiveTrace.class)
})
public interface QueryTrace {
    PassiveTrace PASSIVE = new PassiveTrace();
    NamedWatch PASSIVE_NAMED_WATCH = new PassiveNamedWatch();
    Joiner PASSIVE_JOINER = new PassiveJoiner();

    /**
     * Create a new identifier for a class.
     *
     * @param where Class associated with the identifier
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where) {
        return new Identifier(where.getName());
    }

    /**
     * Create a new identifier for a class and a what
     *
     * @param where Class associated with the identifier
     * @param what String describing what is being traced
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where, String what) {
        return new Identifier(where.getName() + "#" + what);
    }

    /**
     * Create a new identifier for a free text description.
     *
     * @param description Description telling what is being traced.
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(String description) {
        return new Identifier(description);
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
     * @deprecated use {@link Tracing#watch(Identifier)}
     */
    static NamedWatch watch(final Identifier what) {
        return new ActiveNamedWatch(what, Stopwatch.createStarted());
    }

    /**
     * Format the current trace onto the given PrintWriter.
     *
     * @param out {@link java.io.PrintWriter} to format to
     */
    void formatTrace(PrintWriter out);

    /**
     * Format the current trace onto the given PrintWriter with the given prefix.
     *
     * @param out {@link java.io.PrintWriter} to format to
     * @param prefix prefix to prepend
     */
    void formatTrace(PrintWriter out, String prefix);

    /**
     * How long the trace elapsed for.
     *
     * @return microseconds
     */
    long elapsed();

    @JsonTypeName("passive")
    @Data
    class PassiveTrace implements QueryTrace {
        PassiveTrace() {
        }

        @JsonCreator
        public static PassiveTrace create() {
            return PASSIVE;
        }

        @Override
        public void formatTrace(final PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(final PrintWriter out, final String prefix) {
            out.println(prefix + "NO TRACE");
        }

        @JsonIgnore
        @Override
        public long elapsed() {
            return 0L;
        }
    }

    @JsonTypeName("active")
    @Data
    class ActiveTrace implements QueryTrace {
        private final Identifier what;
        private final long elapsed;
        private final List<QueryTrace> children;

        @Override
        public void formatTrace(PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(PrintWriter out, String prefix) {
            out.println(prefix + what + " (in " + readableTime(elapsed) + ")");

            for (final QueryTrace child : children) {
                child.formatTrace(out, prefix + "  ");
            }
        }

        @Override
        public long elapsed() {
            return elapsed;
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
    }

    interface NamedWatch {
        /**
         * End the current watch and return a trace.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end();

        /**
         * End the current watch and return a trace with the given child.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param child Child to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(QueryTrace child);

        /**
         * End the current watch and return a trace with the given children.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param children Children to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(List<QueryTrace> children);

        /**
         * Create a joiner from the current watch.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace.Joiner}
         */
        Joiner joiner();

        /**
         * Create another watch with the same configuration as this watch (active or passive).
         *
         * @param what What is being watched
         * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
         */
        NamedWatch watch(Identifier what);

        /**
         * How long this trace has elapsed for.
         *
         * @return milliseconds since unix epoch
         * @deprecated Makes no distinction between active and passive watches.
         */
        long elapsed();
    }

    @Data
    class ActiveNamedWatch implements NamedWatch {
        private final Identifier what;
        private final Stopwatch w;

        @Override
        public QueryTrace end() {
            return new ActiveTrace(what, elapsed(), ImmutableList.of());
        }

        @Override
        public QueryTrace end(final QueryTrace child) {
            return new ActiveTrace(what, elapsed(), ImmutableList.of(child));
        }

        @Override
        public QueryTrace end(final List<QueryTrace> children) {
            return new ActiveTrace(what, elapsed(), children);
        }

        @Override
        public Joiner joiner() {
            return new ActiveJoiner(this);
        }

        @Override
        public NamedWatch watch(final Identifier what) {
            return new ActiveNamedWatch(what, Stopwatch.createStarted());
        }

        @Override
        public long elapsed() {
            return w.elapsed(TimeUnit.MICROSECONDS);
        }
    }

    @Data
    class PassiveNamedWatch implements NamedWatch {
        @Override
        public QueryTrace end() {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final QueryTrace child) {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final List<QueryTrace> children) {
            return PASSIVE;
        }

        @Override
        public Joiner joiner() {
            return PASSIVE_JOINER;
        }

        @Override
        public NamedWatch watch(final Identifier what) {
            return PASSIVE_NAMED_WATCH;
        }

        @Override
        public long elapsed() {
            return 0L;
        }
    }

    @Data
    class Identifier {
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

    /**
     * Joiner multiple query traces into one.
     */
    interface Joiner {
        /**
         * Add a child trace that should be part of the resulting trace.
         *
         * @param trace Child trace to add
         */
        void addChild(QueryTrace trace);

        /**
         * Create a new trace with a snapshot of the current list of children.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace result();
    }

    /**
     * A joiner that joins the given children into a single trace.
     */
    @Data
    class ActiveJoiner implements Joiner {
        private final QueryTrace.NamedWatch watch;

        private final ImmutableList.Builder<QueryTrace> children = ImmutableList.builder();

        @Override
        public void addChild(final QueryTrace trace) {
            children.add(trace);
        }

        @Override
        public QueryTrace result() {
            return watch.end(children.build());
        }
    }

    /**
     * A joiner that does nothing.
     */
    class PassiveJoiner implements Joiner {
        PassiveJoiner() {
        }

        @Override
        public void addChild(final QueryTrace trace) {
            /* do nothing */
        }

        @Override
        public QueryTrace result() {
            return PASSIVE;
        }
    }
}
