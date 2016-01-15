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

package com.spotify.heroic.coalesce;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.spotify.heroic.ServiceComponent;
import com.spotify.heroic.ServiceComponentModule;
import com.spotify.heroic.coalesce.persistence.FakePersistenceModule;

import java.util.Optional;
import java.util.UUID;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CoalesceModule implements ServiceComponentModule {
    private final Optional<String> id;
    private final String node;
    private final CoalesceConnectionBuilder connection;
    private final CoalescePersistenceModule persistence;

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int index) {
        if (index == 0) {
            return "coalesce";
        }

        return String.format("coalesce#%d", index);
    }

    @Override
    public Module module(final Key<ServiceComponent> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public Managed<CoalesceConnection> connection(final AsyncFramework async) {
                return async.managed(new ManagedSetup<CoalesceConnection>() {
                    @Override
                    public AsyncFuture<CoalesceConnection> construct() throws Exception {
                        return connection.build(async);
                    }

                    @Override
                    public AsyncFuture<Void> destruct(CoalesceConnection value) throws Exception {
                        return value.close();
                    }
                });
            }

            @Provides
            @Singleton
            @Named("id")
            public String id() {
                return id;
            }

            @Provides
            @Singleton
            @Named("node")
            public String node() {
                return node;
            }

            @Override
            public void configure() {
                bind(key).to(CoalesceServiceComponent.class).in(Scopes.SINGLETON);
                expose(key);

                install(persistence.module());
            }
        };
    }

    public static class Builder implements ServiceComponentModule.Builder {
        private Optional<String> id = empty();
        private Optional<String> node = empty();
        private Optional<CoalesceConnectionBuilder> connection = empty();
        private Optional<CoalescePersistenceModule> persistence = empty();

        @JsonCreator
        public Builder(@JsonProperty("id") Optional<String> id,
                @JsonProperty("node") Optional<String> node,
                @JsonProperty("connection") Optional<CoalesceConnectionBuilder> connection,
                @JsonProperty("persistence") Optional<CoalescePersistenceModule> persistence) {
            this.id = id;
            this.node = node;
            this.connection = of(connection
                    .orElseThrow(() -> new IllegalStateException("connection: is required")));
            this.persistence = persistence;
        }

        public Builder id(final String id) {
            this.id = of(id);
            return this;
        }

        public Builder node(final String node) {
            this.node = of(node);
            return this;
        }

        public Builder connection(final CoalesceConnectionBuilder connection) {
            this.connection = of(connection);
            return this;
        }

        public Builder persistence(final CoalescePersistenceModule persistence) {
            this.persistence = of(persistence);
            return this;
        }

        @Override
        public ServiceComponentModule build() {
            final String node = this.node.orElseGet(() -> {
                final String generated = UUID.randomUUID().toString();
                log.warn("No name provided, using generated ({}). "
                        + "This will cause worker to loose state when restarted.");
                return generated;
            });

            final CoalesceConnectionBuilder connection = this.connection
                    .orElseThrow(() -> new IllegalStateException("connection: is required"));

            final CoalescePersistenceModule persistence =
                    this.persistence.orElseGet(FakePersistenceModule::new);

            return new CoalesceModule(id, node, connection, persistence);
        }
    }
}
