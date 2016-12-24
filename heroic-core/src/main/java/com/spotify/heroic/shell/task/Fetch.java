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

package com.spotify.heroic.shell.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@TaskUsage("Fetch a range of data points")
@TaskName("fetch")
public class Fetch implements ShellTask {
    private final MetricManager metrics;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public Fetch(
        MetricManager metrics, @Named("application/json") ObjectMapper mapper, AsyncFramework async
    ) {
        this.metrics = metrics;
        this.mapper = mapper;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;
        final long now = System.currentTimeMillis();

        final Series series = Tasks.parseSeries(mapper, params.series);

        final long start = params.start.map(t -> Tasks.parseInstant(t, now)).orElse(now);
        final long end =
            params.end.map(t -> Tasks.parseInstant(t, now)).orElseGet(() -> defaultEnd(start));

        final DateRange range = new DateRange(start, end);
        final int limit = Math.max(1, params.limit);

        final DateFormat flip = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        final DateFormat point = new SimpleDateFormat("HH:mm:ss.SSS");

        final MetricBackendGroup readGroup = metrics.useOptionalGroup(params.group);
        final MetricType source = MetricType.fromIdentifier(params.source).orElse(MetricType.POINT);

        final QueryOptions options =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.tracing)).build();

        return readGroup
            .fetch(new FetchData.Request(options.getTracing(), series.toMetricKey(), source, range))
            .lazyTransform(result -> {
                outer:
                for (final MetricCollection g : result.getGroups()) {
                    int i = 0;

                    Calendar current = null;
                    Calendar last = null;

                    for (final Metric d : g.getData()) {
                        current = Calendar.getInstance();
                        current.setTime(new Date(d.getTimestamp()));

                        if (flipped(last, current)) {
                            io.out().println(flip.format(current.getTime()));
                        }

                        io
                            .out()
                            .println(
                                String.format("  %s: %s", point.format(new Date(d.getTimestamp())),
                                    d));

                        if (i++ >= limit) {
                            break outer;
                        }

                        last = current;
                    }
                }

                io.out().println("TRACE:");
                result.getTrace().formatTrace(io.out());
                io.out().flush();

                return async.resolved();
            });
    }

    private boolean flipped(Calendar last, Calendar current) {
        if (last == null) {
            return true;
        }

        if (last.get(Calendar.YEAR) != current.get(Calendar.YEAR)) {
            return true;
        }

        if (last.get(Calendar.MONTH) != current.get(Calendar.MONTH)) {
            return true;
        }

        if (last.get(Calendar.DAY_OF_MONTH) != current.get(Calendar.DAY_OF_MONTH)) {
            return true;
        }

        if (last.get(Calendar.HOUR_OF_DAY) != current.get(Calendar.HOUR_OF_DAY)) {
            return true;
        }

        return false;
    }

    private long defaultEnd(long start) {
        return start + TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-s", aliases = {"--series"}, usage = "Series to fetch", metaVar = "<json>")
        private Optional<String> series = Optional.empty();

        @Option(name = "--source", aliases = {"--source"}, usage = "Source to fetch",
            metaVar = "<events|points>")
        private String source = MetricType.POINT.identifier();

        @Option(name = "--start", usage = "Start date", metaVar = "<datetime>")
        private Optional<String> start = Optional.empty();

        @Option(name = "--end", usage = "End date", metaVar = "<datetime>")
        private Optional<String> end = Optional.empty();

        @Option(name = "--limit", usage = "Maximum number of datapoints to fetch",
            metaVar = "<int>")
        private int limit = 1000;

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--tracing", usage = "Enable extensive tracing")
        private boolean tracing = false;
    }

    public static Fetch setup(final CoreComponent core) {
        return DaggerFetch_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        Fetch task();
    }
}
