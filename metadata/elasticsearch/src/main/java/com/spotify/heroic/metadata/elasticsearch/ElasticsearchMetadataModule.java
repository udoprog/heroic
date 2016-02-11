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

package com.spotify.heroic.metadata.elasticsearch;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.BackendTypeFactory;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.DisabledRateLimitedCache;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import eu.toolchain.async.Managed;
import lombok.Data;

@Data
public final class ElasticsearchMetadataModule implements MetadataModule {
    public static final String ELASTICSEARCH_CONFIGURE_PARAM = "elasticsearch.configure";

    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240L;
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic-metadata";

    private final Optional<String> id;
    private final Groups groups;
    private final ManagedConnectionFactory connection;
    private final String templateName;
    private final Double writesPerSecond;
    private final Long writeCacheDurationMinutes;

    private static BackendTypeFactory<MetadataBackend> defaultSetup = MetadataBackendKV.factory();

    private static final Map<String, BackendTypeFactory<MetadataBackend>> backendTypes =
            new HashMap<>();

    static {
        backendTypes.put("kv", defaultSetup);
        backendTypes.put("v1", MetadataBackendV1.factory());
    }

    public static List<String> types() {
        return ImmutableList.copyOf(backendTypes.keySet());
    }

    @JsonIgnore
    private final BackendTypeFactory<MetadataBackend> backendTypeBuilder;

    @JsonCreator
    public ElasticsearchMetadataModule(@JsonProperty("id") Optional<String> id,
            @JsonProperty("groups") Optional<Groups> groups,
            @JsonProperty("connection") Optional<ManagedConnectionFactory> connection,
            @JsonProperty("writesPerSecond") Optional<Double> writesPerSecond,
            @JsonProperty("writeCacheDurationMinutes") Optional<Long> writeCacheDurationMinutes,
            @JsonProperty("templateName") Optional<String> templateName,
            @JsonProperty("backendType") Optional<String> backendType) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.connection = connection.orElseGet(ManagedConnectionFactory::buildDefault);
        this.writesPerSecond = writesPerSecond.orElse(DEFAULT_WRITES_PER_SECOND);
        this.writeCacheDurationMinutes =
                writeCacheDurationMinutes.orElse(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.templateName = templateName.orElse(DEFAULT_TEMPLATE_NAME);
        this.backendTypeBuilder =
                backendType.flatMap(bt -> ofNullable(backendTypes.get(bt))).orElse(defaultSetup);
    }

    @Override
    public Module module(final Key<MetadataBackend> key, final String id) {
        final BackendType<MetadataBackend> backendType = backendTypeBuilder.setup();

        return new PrivateModule() {
            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            public Groups groups() {
                return groups;
            }

            @Provides
            @Inject
            public Managed<Connection> connection(ManagedConnectionFactory builder)
                    throws IOException {
                return builder.construct(templateName, backendType.mappings());
            }

            @Provides
            @Singleton
            @Named("configure")
            public boolean configure(ExtraParameters params) {
                return params.contains(ExtraParameters.CONFIGURE)
                        || params.contains(ELASTICSEARCH_CONFIGURE_PARAM);
            }

            @Provides
            @Singleton
            public RateLimitedCache<Pair<String, HashCode>> writeCache() throws IOException {
                final Cache<Pair<String, HashCode>, Boolean> cache = CacheBuilder.newBuilder()
                        .concurrencyLevel(4)
                        .expireAfterWrite(writeCacheDurationMinutes, TimeUnit.MINUTES).build();

                if (writesPerSecond <= 0d) {
                    return new DisabledRateLimitedCache<>(cache.asMap());
                }

                return new DefaultRateLimitedCache<>(cache.asMap(),
                        RateLimiter.create(writesPerSecond));
            }

            @Override
            protected void configure() {
                bind(ManagedConnectionFactory.class).toInstance(connection);
                bind(key).to(backendType.type()).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("elasticsearch#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<ManagedConnectionFactory> connection = empty();
        private Optional<Double> writesPerSecond = empty();
        private Optional<Long> writeCacheDurationMinutes = empty();
        private Optional<String> templateName = empty();
        private Optional<String> backendType = empty();

        public Builder id(final String id) {
            checkNotNull(id, "id");
            this.id = of(id);
            return this;
        }

        public Builder groups(final Groups groups) {
            checkNotNull(groups, "groups");
            this.groups = of(groups);
            return this;
        }

        public Builder connection(final ManagedConnectionFactory connection) {
            checkNotNull(connection, "connection");
            this.connection = of(connection);
            return this;
        }

        public Builder writesPerSecond(final double writesPerSecond) {
            checkNotNull(writesPerSecond, "writesPerSecond");
            this.writesPerSecond = of(writesPerSecond);
            return this;
        }

        public Builder writeCacheDurationMinutes(final long writeCacheDurationMinutes) {
            checkNotNull(writeCacheDurationMinutes, "writeCacheDurationMinutes");
            this.writeCacheDurationMinutes = of(writeCacheDurationMinutes);
            return this;
        }

        public Builder templateName(final String templateName) {
            checkNotNull(templateName, "templateName");
            this.templateName = of(templateName);
            return this;
        }

        public Builder backendType(final String backendType) {
            checkNotNull(backendType, "backendType");
            this.backendType = of(backendType);
            return this;
        }

        public ElasticsearchMetadataModule build() {
            return new ElasticsearchMetadataModule(id, groups, connection, writesPerSecond,
                    writeCacheDurationMinutes, templateName, backendType);
        }
    }
}
