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

package com.spotify.heroic.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.metric.WriteMetric;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public abstract class AbstractMetricBackendIT {
    protected abstract Optional<MetricModule> setupModule();

    protected final Series s1 = Series.of("s1", ImmutableMap.of("id", "s1"));
    protected final Series s2 = Series.of("s2", ImmutableMap.of("id", "s2"));

    protected Optional<MetricModule> module;
    protected HeroicCoreInstance core;
    protected MetricBackend backend;

    @Before
    public void setup() throws Exception {
        module = setupModule();

        // figure out a better way to do this
        if (!module.isPresent()) {
            return;
        }

        final MetricManagerModule.Builder metric =
            MetricManagerModule.builder().backends(ImmutableList.of(module.get()));

        final HeroicConfig.Builder fragment = HeroicConfig.builder().metrics(metric);

        core = HeroicCore
            .builder()
            .setupShellServer(false)
            .setupService(false)
            .configFragment(fragment)
            .build()
            .newInstance();

        core.start().get();

        backend = core
            .inject(c -> c
                .metricManager()
                .groupSet()
                .inspectAll()
                .stream()
                .map(GroupMember::getMember)
                .findFirst())
            .orElseThrow(() -> new IllegalStateException("Failed to find backend"));
    }

    @After
    public void teardown() throws Exception {
        module = setupModule();

        // figure out a better way to do this
        if (!module.isPresent()) {
            return;
        }

        core.shutdown().get();
    }

    @Test
    public void testWrite() throws Exception {
        // figure out a better way to do this
        if (!module.isPresent()) {
            return;
        }

        // write and read data back
        final MetricCollection points = Data.points().p(100000L, 42D).build();
        backend.write(new WriteMetric.Request(Tracing.disabled(), s1, points)).get();
        FetchData data = backend
            .fetch(new FetchData.Request(MetricType.POINT, s1, new DateRange(10000L, 200000L),
                Tracing.disabled()), FetchQuotaWatcher.NO_QUOTA)
            .get();

        assertEquals(ImmutableSet.of(points), ImmutableSet.copyOf(data.getGroups()));
    }
}
