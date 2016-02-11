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

package com.spotify.heroic.rpc.nativerpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import javax.inject.Named;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.RpcProtocolModule;

import java.net.InetSocketAddress;
import java.util.Optional;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.ResolvableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.Data;

@Data
public class NativeRpcProtocolModule implements RpcProtocolModule {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 0;
    private static final int DEFAULT_PARENT_THREADS = 2;
    private static final int DEFAULT_CHILD_THREADS = 100;
    private static final int DEFAULT_MAX_FRAME_SIZE = 10 * 1000000;
    private static final long DEFAULT_SEND_TIMEOUT = 5000;
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 1000;

    private final InetSocketAddress address;
    private final int parentThreads;
    private final int childThreads;
    private final int maxFrameSize;
    private final long sendTimeout;
    private final long heartbeatInterval;
    private final NativeEncoding encoding;

    @JsonCreator
    public NativeRpcProtocolModule(@JsonProperty("host") String host,
            @JsonProperty("port") Integer port,
            @JsonProperty("parentThreads") Integer parentThreads,
            @JsonProperty("childThreads") Integer childThreads,
            @JsonProperty("maxFrameSize") Integer maxFrameSize,
            @JsonProperty("heartbeatInterval") Long heartbeatInterval,
            @JsonProperty("sendTimeout") Long sendTimeout,
            @JsonProperty("encoding") Optional<NativeEncoding> encoding) {
        this.address = new InetSocketAddress(Optional.ofNullable(host).orElse(DEFAULT_HOST),
                Optional.ofNullable(port).orElse(DEFAULT_PORT));
        this.parentThreads = Optional.ofNullable(parentThreads).orElse(DEFAULT_PARENT_THREADS);
        this.childThreads = Optional.ofNullable(childThreads).orElse(DEFAULT_CHILD_THREADS);
        this.maxFrameSize = Optional.ofNullable(maxFrameSize).orElse(DEFAULT_MAX_FRAME_SIZE);
        this.heartbeatInterval =
                Optional.ofNullable(heartbeatInterval).orElse(DEFAULT_HEARTBEAT_INTERVAL);
        this.sendTimeout = Optional.ofNullable(sendTimeout).orElse(DEFAULT_SEND_TIMEOUT);
        this.encoding = encoding.orElse(NativeEncoding.GZIP);
    }

    @Override
    public Module module(final Key<RpcProtocol> key, final HeroicConfiguration options) {
        return new PrivateModule() {
            @Provides
            @Singleton
            @Named("bindFuture")
            public ResolvableFuture<InetSocketAddress> bindFuture(final AsyncFramework async) {
                return async.future();
            }

            @Provides
            @Singleton
            @Named("boss")
            public EventLoopGroup bossGroup() {
                return new NioEventLoopGroup(parentThreads);
            }

            @Provides
            @Singleton
            @Named("worker")
            public EventLoopGroup workerGroup() {
                return new NioEventLoopGroup(childThreads);
            }

            @Provides
            @Singleton
            public Timer timer() {
                return new HashedWheelTimer(
                        new ThreadFactoryBuilder().setNameFormat("nativerpc-timer-%d").build());
            }

            @Provides
            @Singleton
            public NativeEncoding encoding() {
                return encoding;
            }

            @Override
            protected void configure() {
                bind(key).toInstance(new NativeRpcProtocol(DEFAULT_PORT, maxFrameSize, sendTimeout,
                        heartbeatInterval));

                if (!options.isDisableLocal()) {
                    bind(NativeRpcProtocolServer.class)
                            .toInstance(new NativeRpcProtocolServer(address, maxFrameSize));
                }

                expose(key);
            }
        };
    }

    @Override
    public String scheme() {
        return "nativerpc";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private int parentThreads = DEFAULT_PARENT_THREADS;
        private int childThreads = DEFAULT_CHILD_THREADS;
        private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
        private long sendTimeout = DEFAULT_SEND_TIMEOUT;
        private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
        private NativeEncoding encoding = NativeEncoding.GZIP;

        public Builder host(final String host) {
            this.host = host;
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public Builder parentThreads(final int parentThreads) {
            this.parentThreads = parentThreads;
            return this;
        }

        public Builder childThreads(final int childThreads) {
            this.childThreads = childThreads;
            return this;
        }

        public Builder maxFrameSize(final int maxFrameSize) {
            this.maxFrameSize = maxFrameSize;
            return this;
        }

        public Builder sendTimeout(final long sendtimeout) {
            this.sendTimeout = sendtimeout;
            return this;
        }

        public Builder heartbeatInterval(final int heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder encoding(final NativeEncoding encoding) {
            this.encoding = encoding;
            return this;
        }

        public NativeRpcProtocolModule build() {
            return new NativeRpcProtocolModule(host, port, parentThreads, childThreads,
                    maxFrameSize, sendTimeout, heartbeatInterval, Optional.of(encoding));
        }
    }
}
