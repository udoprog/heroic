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

package com.spotify.heroic.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ServerConnector {
    private final Optional<InetSocketAddress> address;
    private final List<Connection> factories;
    private final Optional<String> defaultProtocol;

    public static List<Connection.Builder> defaultFactories() {
        return ImmutableList.of(HttpConnection.builder(), Http2CConnection.builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder {
        private Optional<InetSocketAddress> address = Optional.empty();
        private Optional<List<Connection.Builder>> factories = Optional.empty();
        private Optional<String> defaultProtocol = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("address") Optional<InetSocketAddress> address,
            @JsonProperty("factories") Optional<List<Connection.Builder>> connections,
            @JsonProperty("defaultProtocol") Optional<String> defaultProtocol
        ) {
            this.address = address;
            this.factories = connections;
            this.defaultProtocol = defaultProtocol;
        }

        public Builder address(final InetSocketAddress address) {
            this.address = Optional.of(address);
            return this;
        }

        public Builder factories(final List<Connection.Builder> factories) {
            this.factories = Optional.of(factories);
            return this;
        }

        public ServerConnector build() {
            final List<Connection> factories = ImmutableList.copyOf(this.factories
                .orElseGet(ServerConnector::defaultFactories)
                .stream()
                .map(f -> f.build())
                .iterator());

            return new ServerConnector(address, factories, defaultProtocol);
        }
    }
}
