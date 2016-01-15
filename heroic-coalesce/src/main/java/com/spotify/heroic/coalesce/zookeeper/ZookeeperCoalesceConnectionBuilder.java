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

package com.spotify.heroic.coalesce.zookeeper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.coalesce.CoalesceConnection;
import com.spotify.heroic.coalesce.CoalesceConnectionBuilder;
import com.spotify.heroic.common.Duration;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@JsonTypeName("zookeeper")
public class ZookeeperCoalesceConnectionBuilder implements CoalesceConnectionBuilder {
    public static final String DEFAULT_ZOOKEEPER = "localhost";
    public static final Duration DEFAULT_BLOCK_UNTIL_CONNECTED = new Duration(10, TimeUnit.SECONDS);
    public static final String DEFAULT_NAMESPACE = "heroic-coalesce";

    private final String zookeeper;
    private final Duration blockUntilConnected;
    private final Optional<Duration> sessionTimeout;
    private final Optional<Duration> connectionTimeout;
    private final String namespace;

    @JsonCreator
    public ZookeeperCoalesceConnectionBuilder(
            @JsonProperty("zookeeper") final Optional<String> zookeeper,
            @JsonProperty("blockUntilConnected") final Optional<Duration> blockUntilConnected,
            @JsonProperty("sessionTimeout") final Optional<Duration> sessionTimeout,
            @JsonProperty("connectionTimeout") final Optional<Duration> connectionTimeout,
            @JsonProperty("namespace") final Optional<String> namespace) {
        this.zookeeper = zookeeper.orElse(DEFAULT_ZOOKEEPER);
        this.blockUntilConnected = blockUntilConnected.orElse(DEFAULT_BLOCK_UNTIL_CONNECTED);
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.namespace = namespace.orElse(DEFAULT_NAMESPACE);
    }

    @Override
    public AsyncFuture<CoalesceConnection> build(final AsyncFramework async) {
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFrameworkFactory.Builder clientBuilder = CuratorFrameworkFactory.builder()
                .connectString(zookeeper).retryPolicy(retryPolicy).namespace(namespace);

        sessionTimeout.ifPresent(
                s -> clientBuilder.sessionTimeoutMs((int) s.convert(TimeUnit.MILLISECONDS)));
        connectionTimeout.ifPresent(
                s -> clientBuilder.connectionTimeoutMs((int) s.convert(TimeUnit.MILLISECONDS)));

        final CuratorFramework client = clientBuilder.build();

        client.start();

        return async.resolved(new ZookeeperCoalesceConnection(async, client));
    }
}
