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

package com.spotify.heroic.metric.filesystem;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.DynamicModuleId;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.ModuleId;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.filesystem.io.FilesFramework;
import com.spotify.heroic.metric.filesystem.io.RealFilesFramework;
import com.spotify.heroic.metric.filesystem.wal.DisabledWalConfig;
import com.spotify.heroic.metric.filesystem.wal.WalBuilder;
import com.spotify.heroic.metric.filesystem.wal.WalConfig;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.inject.Named;
import javax.inject.Singleton;
import lombok.Data;

@Module
@Data
@ModuleId("fs")
public final class FilesystemMetricModule implements MetricModule, DynamicModuleId {
    public static final String DEFAULT_GROUP = "fs";
    public static final Path DEFAULT_DIRECTORY = Paths.get("./storage");
    public static final Duration DEFAULT_SEGMENT_WIDTH = Duration.of(1, TimeUnit.HOURS);
    public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.of(5, TimeUnit.SECONDS);
    public static final int DEFAULT_MAX_TRANSACTIONS_PER_FLUSH = 100;
    public static final int DEFAULT_MAX_PENDING_TRANSACTIONS = 10000;
    public static final int DEFAULT_TRANSACTION_PARALLELISM_PER_REQUEST = 2;
    public static final long DEFAULT_SEGMENT_CACHE_SIZE = 100;
    public static final boolean DEFAULT_MEMORY = true;

    private final Optional<String> id;
    private final Groups groups;
    private final Path storagePath;
    private final Duration segmentWidth;
    private final long segmentCacheSize;
    private final Duration flushInterval;
    private final int maxTransactionsPerFlush;
    private final int maxPendingTransactions;
    private final int transactionParallelismPerRequest;
    private final Compression compression;
    private final WalConfig walConfig;
    private final FilesFramework files;
    private final boolean useMemoryCache;

    @JsonCreator
    public FilesystemMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("storagePath") Optional<Path> storagePath,
        @JsonProperty("segmentWidth") final Optional<Duration> segmentWidth,
        @JsonProperty("segmentCacheSize") final Optional<Long> segmentCacheSize,
        @JsonProperty("flushInterval") final Optional<Duration> flushInterval,
        @JsonProperty("maxTransactionsPerFlush") final Optional<Integer> maxTransactionsPerFlush,
        @JsonProperty("maxPendingTransactions") final Optional<Integer> maxPendingTransactions,
        @JsonProperty("transactionParallelismPerRequest")
        final Optional<Integer> transactionParallelismPerRequest,
        @JsonProperty("compression") Optional<Compression> compression,
        @JsonProperty("wal") Optional<WalConfig> wal,
        @JsonProperty("files") Optional<FilesFramework> files,
        @JsonProperty("useMemoryCache") Optional<Boolean> useMemoryCache
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.storagePath = storagePath.orElse(DEFAULT_DIRECTORY);
        this.segmentWidth = segmentWidth.orElse(DEFAULT_SEGMENT_WIDTH);
        this.segmentCacheSize = segmentCacheSize.orElse(DEFAULT_SEGMENT_CACHE_SIZE);
        this.flushInterval = flushInterval.orElse(DEFAULT_FLUSH_INTERVAL);
        this.maxTransactionsPerFlush =
            maxTransactionsPerFlush.orElse(DEFAULT_MAX_TRANSACTIONS_PER_FLUSH);
        this.maxPendingTransactions =
            maxPendingTransactions.orElse(DEFAULT_MAX_PENDING_TRANSACTIONS);
        this.transactionParallelismPerRequest =
            transactionParallelismPerRequest.orElse(DEFAULT_TRANSACTION_PARALLELISM_PER_REQUEST);
        this.compression = compression.orElse(Compression.NONE);
        this.walConfig = wal.orElseGet(DisabledWalConfig::new);
        this.files = files.orElseGet(RealFilesFramework::new);
        this.useMemoryCache = useMemoryCache.orElse(DEFAULT_MEMORY);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        return DaggerFilesystemComponent
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .filesystemMetricModule(this)
            .build();
    }

    @Provides
    @Singleton
    public Groups groups() {
        return groups;
    }

    @Provides
    @Singleton
    @Named("storagePath")
    public Path storagePath() {
        return storagePath;
    }

    @Provides
    @Singleton
    @Named("segmentWidth")
    public Duration segmentWidth() {
        return segmentWidth;
    }

    @Provides
    @Singleton
    @Named("segmentCacheSize")
    public long segmentCacheSize() {
        return segmentCacheSize;
    }

    @Provides
    @Singleton
    @Named("flushInterval")
    public Duration flushInterval() {
        return flushInterval;
    }

    @Provides
    @Singleton
    @Named("maxTransactionsPerFlush")
    public int maxTransactionsPerFlush() {
        return maxTransactionsPerFlush;
    }

    @Provides
    @Singleton
    @Named("maxPendingTransactions")
    public int maxPendingTransactions() {
        return maxPendingTransactions;
    }

    @Provides
    @Singleton
    @Named("transactionParallelismPerRequest")
    public int transactionParallelismPerRequest() {
        return transactionParallelismPerRequest;
    }

    @Provides
    @Singleton
    public SerializerFramework serializerFramework() {
        return TinySerializer.builder().useCompactSize(true).build();
    }

    @Provides
    @Singleton
    public LifeCycle life(final LifeCycleManager manager, final FilesystemBackend backend) {
        return manager.build(backend);
    }

    @Provides
    @Singleton
    @Named("writers")
    public ExecutorService writers() {
        return Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("heroic-fs-writer-%d").build());
    }

    @Provides
    @Singleton
    public Compression compression() {
        return compression;
    }

    @Provides
    @Singleton
    public WalConfig writeAheadLog() {
        return walConfig;
    }

    @Provides
    @Singleton
    public FilesFramework files() {
        return files;
    }

    @Provides
    @Singleton
    @Named("useMemoryCache")
    public boolean useMemoryCache() {
        return useMemoryCache;
    }

    @Provides
    @Singleton
    WalBuilder<Transaction> walBuilder(
        final SerializerFramework serializer, final FilesFramework files,
        final PrimaryComponent primary
    ) {
        final WalConfig.Dependencies dependencies = new WalConfig.Dependencies() {
            @Override
            public Path storagePath() {
                return storagePath;
            }

            @Override
            public FilesFramework files() {
                return files;
            }
        };

        return receiver -> walConfig.newWriteAheadLog(receiver,
            new Transaction_Serializer(serializer), primary, dependencies);
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<Path> storagePath = empty();
        private Optional<Duration> segmentWidth = empty();
        private Optional<Long> segmentCacheSize = empty();
        private Optional<Duration> flushInterval = empty();
        private Optional<Integer> maxTransactionsPerFlush = empty();
        private Optional<Integer> transactionParallelismPerRequest = empty();
        private Optional<Integer> maxPendingTransactions = empty();
        private Optional<Compression> compression = empty();
        private Optional<WalConfig> wal = empty();
        private Optional<FilesFramework> files = empty();
        private Optional<Boolean> memory = empty();

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        /**
         * Path to store snapshot.
         */
        public Builder storagePath(Path storagePath) {
            this.storagePath = of(storagePath);
            return this;
        }

        /**
         * The segmentWidth of a single segment.
         */
        public Builder segmentWidth(Duration segmentWidth) {
            this.segmentWidth = of(segmentWidth);
            return this;
        }

        /**
         * The maximum number of segments to cache in memory.
         *
         * Segments are evicted from the cache in LRU order.
         */
        public Builder segmentCacheSize(long segmentCacheSize) {
            this.segmentCacheSize = of(segmentCacheSize);
            return this;
        }

        /**
         * Maximum amount of time permitted to spend flushing.
         */
        public Builder flushInterval(Duration flushInterval) {
            this.flushInterval = of(flushInterval);
            return this;
        }

        /**
         * Max number of transactions to apply per flush.
         */
        public Builder maxTransactionsPerFlush(int maxTransactionsPerFlush) {
            this.maxTransactionsPerFlush = of(maxTransactionsPerFlush);
            return this;
        }

        /**
         * Max number of pending flushes to keep in memory.
         */
        public Builder maxPendingTransactions(int maxPendingTransactions) {
            this.maxPendingTransactions = of(maxPendingTransactions);
            return this;
        }

        /**
         * Max number of transactions a single request is permitted to commit in parallel.
         */
        public Builder transactionParallelismPerRequest(int transactionParallelismPerRequest) {
            this.transactionParallelismPerRequest = of(transactionParallelismPerRequest);
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = of(compression);
            return this;
        }

        public Builder wal(WalConfig walConfig) {
            this.wal = of(walConfig);
            return this;
        }

        public Builder files(FilesFramework files) {
            this.files = of(files);
            return this;
        }

        /**
         * Configured if intermediate storage in-memory should be used or not.
         *
         * This allows metrics to become available faster than without it.
         */
        public Builder memory(boolean memory) {
            this.memory = of(memory);
            return this;
        }

        public FilesystemMetricModule build() {
            return new FilesystemMetricModule(id, groups, storagePath, segmentWidth,
                segmentCacheSize, flushInterval, maxTransactionsPerFlush, maxPendingTransactions,
                transactionParallelismPerRequest, compression, wal, files, memory);
        }
    }
}
