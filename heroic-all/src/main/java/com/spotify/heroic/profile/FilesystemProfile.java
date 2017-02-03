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

package com.spotify.heroic.profile;

import static com.spotify.heroic.ParameterSpecification.parameter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.memory.MemoryMetadataModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.filesystem.Compression;
import com.spotify.heroic.metric.filesystem.FilesystemMetricModule;
import com.spotify.heroic.metric.filesystem.wal.FileWalConfig;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.memory.MemorySuggestModule;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class FilesystemProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final HeroicConfig.Builder builder = HeroicConfig.builder();

        final ImmutableList.Builder<SuggestModule> suggest = ImmutableList.builder();
        final ImmutableList.Builder<MetadataModule> metadata = ImmutableList.builder();

        suggest.add(MemorySuggestModule.builder().build());
        metadata.add(MemoryMetadataModule.builder().build());

        // @formatter:off
        final FilesystemMetricModule.Builder module = FilesystemMetricModule.builder();

        module.wal(new FileWalConfig());

        params.get("storagePath").map(Paths::get).ifPresent(module::storagePath);

        params
            .get("compression")
            .map(String::toUpperCase)
            .map(Compression::valueOf)
            .ifPresent(module::compression);

        return builder
            .cluster(
                ClusterManagerModule.builder()
                    .tags(ImmutableMap.of("site", "local"))
            )
            .metrics(
                MetricManagerModule.builder()
                    .backends(ImmutableList.<MetricModule>of(
                        module.build()
                    ))
            )
            .metadata(MetadataManagerModule.builder().backends(metadata.build()))
            .suggest(SuggestManagerModule.builder().backends(suggest.build()));
        // @formatter:on
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("fs");
    }

    @Override
    public String description() {
        return "Configures filesystem backend";
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("storagePath", "Path to use for storage"),
            parameter("compression", "Compression method to use")
        );
        // @formatter:on
    }
}
