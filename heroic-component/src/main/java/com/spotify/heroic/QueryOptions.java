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
import com.spotify.heroic.metric.QueryTrace;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

@RequiredArgsConstructor
@EqualsAndHashCode(of = {"tracing", "fetchSize"})
public class QueryOptions {
    public static final boolean DEFAULT_TRACING = false;
    public static final boolean DEFAULT_COMPACT_TRACING = false;

    public static final QueryOptions DEFAULTS =
        new QueryOptions(Optional.empty(), Optional.empty(), Optional.empty());

    /**
     * Indicates if tracing is enabled.
     * <p>
     * Traces queries will include a {@link QueryTrace} object that indicates detailed timings of
     * the query.
     *
     * @return {@code true} if tracing is enabled.
     */
    private final boolean tracing;

    /**
     * Generate a more compact, but less accurate trace.
     */
    private final boolean compactTracing;

    /**
     * The number of entries to fetch for every batch.
     */
    private final Optional<Integer> fetchSize;

    @JsonCreator
    public QueryOptions(
        @JsonProperty("tracing") Optional<Boolean> tracing,
        @JsonProperty("compactTracing") Optional<Boolean> compactTracing,
        @JsonProperty("fetchSize") Optional<Integer> fetchSize
    ) {
        this.tracing = tracing.orElse(DEFAULT_TRACING);
        this.compactTracing = compactTracing.orElse(DEFAULT_COMPACT_TRACING);
        this.fetchSize = fetchSize;
    }

    public boolean isTracing() {
        return tracing;
    }

    public boolean isCompactTracing() {
        return compactTracing;
    }

    public Optional<Integer> getFetchSize() {
        return fetchSize;
    }

    public static QueryOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public QueryOptions withCompactTracing(final boolean compactTracing) {
        return new QueryOptions(tracing, compactTracing, fetchSize);
    }

    public static class Builder {
        private Optional<Boolean> tracing = Optional.empty();
        private Optional<Boolean> compactTracing = Optional.empty();
        private Optional<Integer> fetchSize = Optional.empty();

        public Builder tracing(boolean tracing) {
            this.tracing = Optional.of(tracing);
            return this;
        }

        public Builder compactTracing(boolean compactTracing) {
            this.compactTracing = Optional.of(compactTracing);
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = Optional.of(fetchSize);
            return this;
        }

        public QueryOptions build() {
            return new QueryOptions(tracing, compactTracing, fetchSize);
        }
    }
}
