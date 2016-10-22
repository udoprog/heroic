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

package com.spotify.heroic.ingestion;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;

@Data
public class IngestionOptions {
    public static final boolean DEFAULT_TRACING = false;

    public static IngestionOptions DEFAULTS = builder().build();

    /**
     * Indicates if tracing is enabled.
     * <p>
     * Traces queries will include a {@link com.spotify.heroic.metric.QueryTrace} object that
     * indicates detailed timings of the request.
     */
    private final boolean tracing;

    public static IngestionOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    @AllArgsConstructor
    public static class Builder {
        private Optional<Boolean> tracing = Optional.empty();

        public Builder() {
        }

        public Builder tracing(boolean tracing) {
            this.tracing = Optional.of(tracing);
            return this;
        }

        public IngestionOptions build() {
            final boolean tracing = this.tracing.orElse(DEFAULT_TRACING);
            return new IngestionOptions(tracing);
        }
    }
}
