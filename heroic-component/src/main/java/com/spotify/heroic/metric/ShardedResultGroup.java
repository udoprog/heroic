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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.common.Series;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public final class ShardedResultGroup {
    private final Map<String, String> shard;
    private final Map<String, String> key;
    private final Set<Series> series;
    private final MetricCollection metrics;
    private final long cadence;

    public boolean isEmpty() {
        return metrics.isEmpty();
    }

    public static MultiSummary summarize(List<ShardedResultGroup> resultGroups) {
        Multimap<String, String> shardSummary = HashMultimap.create();
        Multimap<String, String> keySummary = HashMultimap.create();
        SeriesSetsSummarizer seriesSummarizer = new SeriesSetsSummarizer();
        MetricCollection.MultiSummary.Builder groupSummarizer =
            new MetricCollection.MultiSummary.Builder();
        Optional<Long> cadence = Optional.empty();

        for (ShardedResultGroup rg : resultGroups) {
            // shard
            for (Map.Entry<String, String> e : rg.getShard().entrySet()) {
                shardSummary.put(e.getKey(), e.getValue());
            }

            // key
            for (Map.Entry<String, String> e : rg.getKey().entrySet()) {
                keySummary.put(e.getKey(), e.getValue());
            }

            seriesSummarizer.add(rg.getSeries());
            groupSummarizer.add(rg.getMetrics());

            cadence = Optional.of(rg.getCadence());
        }

        SeriesSetsSummarizer.Summary seriesSummary = seriesSummarizer.end();
        MetricCollection.MultiSummary groupSummary = groupSummarizer.end();
        return new MultiSummary(shardSummary.asMap(), keySummary.asMap(), seriesSummary,
            groupSummary, cadence);
    }

    @Data
    public static class MultiSummary {
        private final Map<String, Collection<String>> shardSummary;
        private final Map<String, Collection<String>> keySummary;
        private final SeriesSetsSummarizer.Summary seriesSummary;
        private final MetricCollection.MultiSummary groupSummary;
        private final Optional<Long> cadence;
    }
}
