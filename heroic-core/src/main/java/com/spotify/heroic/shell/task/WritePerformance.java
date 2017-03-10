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

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.generator.Generator;
import com.spotify.heroic.generator.GeneratorManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.kohsuke.args4j.Option;

@TaskUsage("Perform performance testing")
@TaskName("write-performance")
public class WritePerformance implements ShellTask {
    public static final TimeUnit UNIT = TimeUnit.MICROSECONDS;

    private final Clock clock;
    private final MetricManager metrics;
    private final AsyncFramework async;
    private final GeneratorManager generator;

    @Inject
    public WritePerformance(
        Clock clock, MetricManager metrics, AsyncFramework async, GeneratorManager generator
    ) {
        this.clock = clock;
        this.metrics = metrics;
        this.async = async;
        this.generator = generator;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    final Joiner joiner = Joiner.on(" ");

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Generator generator =
            this.generator.findGenerator(params.generator).orElseThrow(() -> {
                return new IllegalArgumentException("No such generator: " + params.generator);
            });

        final Date now = new Date();

        final List<Series> series = generateSeries(params.series);

        final long startRange = now.getTime() - params.history.toMilliseconds();
        final long endRange = now.getTime();
        final DateRange range = new DateRange(startRange, endRange);

        final List<MetricBackend> targets = resolveTargets(params.targets);

        final List<WriteMetric.Request> input = new ArrayList<>();

        for (final Series s : series) {
            final MetricCollection collection = generator.generate(s, range);
            input.add(new WriteMetric.Request(s, collection));
        }

        final List<CollectedTimes> warmup = new ArrayList<>();

        for (int i = 0; i < params.warmup; i++) {
            final Stopwatch stopwatch = Stopwatch.createStarted();

            io.out().println(String.format("Warmup step %d/%d", (i + 1), params.warmup));

            final List<Callable<AsyncFuture<Times>>> writes =
                buildWrites(targets, input, params, stopwatch);

            warmup.add(collectWrites(io.out(), writes, params.parallelism).get());
            // try to trick the JVM not to optimize out any steps
            warmup.clear();
        }

        int totalWrites = 0;

        for (final WriteMetric.Request w : input) {
            totalWrites += (w.getData().size() * params.writes);
        }

        final List<CollectedTimes> all = new ArrayList<>();
        final List<Double> writesPerSecond = new ArrayList<>();
        final List<Double> runtimes = new ArrayList<>();

        for (int i = 0; i < params.loop; i++) {
            io.out().println(String.format("Running step %d/%d", (i + 1), params.loop));

            final Stopwatch stopwatch = Stopwatch.createStarted();

            final List<Callable<AsyncFuture<Times>>> writes =
                buildWrites(targets, input, params, stopwatch);
            all.add(collectWrites(io.out(), writes, params.parallelism).get());

            final long totalRuntime = stopwatch.elapsed(UNIT);

            final double totalRuntimeSeconds =
                totalRuntime / (double) UNIT.convert(1, TimeUnit.SECONDS);

            writesPerSecond.add(totalWrites / totalRuntimeSeconds);
            runtimes.add(totalRuntimeSeconds);
        }

        final CollectedTimes times = merge(all);

        io.out().println(String.format("Failed: %d write(s)", times.errors));
        io.out().println(String.format("Times: %s", convertList(runtimes)));
        io.out().println(String.format("Write/s: %s", convertList(writesPerSecond)));
        io.out().println();

        printHistogram("Batch Times", io.out(), times.runTimes.build());
        io.out().println();

        printHistogram("Individual Times", io.out(), times.executionTimes.build());
        io.out().flush();

        return async.resolved();
    }

    private String convertList(final List<Double> values) {
        return joiner.join(values.stream().map(v -> String.format("%.2f s", v)).iterator());
    }

    private CollectedTimes merge(List<CollectedTimes> all) {
        final Histogram.Builder runTimes = Histogram.builder();
        final Histogram.Builder executionTimes = Histogram.builder();
        int errors = 0;

        for (final CollectedTimes time : all) {
            runTimes.addAll(time.runTimes);
            executionTimes.addAll(time.executionTimes);
            errors += time.errors;
        }

        return new CollectedTimes(runTimes, executionTimes, errors);
    }

    private List<MetricBackend> resolveTargets(List<String> targets) {
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("'targets' is empty, add some with --target");
        }

        final List<MetricBackend> backends = new ArrayList<>();

        for (final String target : targets) {
            backends.add(metrics.useGroup(target));
        }

        return backends;
    }

    private void printHistogram(
        String title, final PrintWriter out, final Histogram timings
    ) {
        out.println(String.format("%s:", title));
        out.println(String.format(" total: %d write(s)", timings.getCount()));

        timings.getMean().ifPresent(mean -> {
            out.println(String.format("    mean: %s", formatTime(mean, UNIT)));
        });

        timings.getMedian().ifPresent(value -> {
            out.println(String.format("  median: %s", formatTime(value, UNIT)));
        });

        timings.getP75().ifPresent(value -> {
            out.println(String.format("    75th: %s", formatTime(value, UNIT)));
        });

        timings.getP99().ifPresent(value -> {
            out.println(String.format("    99th: %s", formatTime(value, UNIT)));
        });
    }

    private static final List<Pair<TimeUnit, String>> FORMAT_UNITS =
        ImmutableList.of(Pair.of(TimeUnit.MINUTES, "m"), Pair.of(TimeUnit.SECONDS, "s"),
            Pair.of(TimeUnit.MILLISECONDS, "ms"), Pair.of(TimeUnit.MICROSECONDS, "us"),
            Pair.of(TimeUnit.NANOSECONDS, "ns"));

    private String formatTime(final long input, final TimeUnit unit) {
        final double time = (double) input;
        return formatTime(time, unit);
    }

    private String formatTime(final double time, final TimeUnit unit) {
        for (final Pair<TimeUnit, String> formatUnit : FORMAT_UNITS) {
            final double factor = (double) unit.convert(1, formatUnit.getLeft());

            if (time / factor > 1.0d) {
                return String.format("%.3f %s", time / factor, formatUnit.getRight());
            }
        }

        throw new IllegalArgumentException("input");
    }

    private List<Callable<AsyncFuture<Times>>> buildWrites(
        List<MetricBackend> targets, List<WriteMetric.Request> input, final Parameters params,
        final Stopwatch stopwatch
    ) {
        final List<Callable<AsyncFuture<Times>>> writes = new ArrayList<>();

        int request = 0;

        for (int i = 0; i < params.writes; i++) {
            for (final WriteMetric.Request w : input) {
                final MetricBackend target = targets.get(request++ % targets.size());

                writes.add(() -> target.write(w).directTransform(result -> {
                    return new Times(result.getTimes(), stopwatch.elapsed(UNIT));
                }));
            }
        }

        return writes;
    }

    private AsyncFuture<CollectedTimes> collectWrites(
        final PrintWriter out, Collection<Callable<AsyncFuture<Times>>> writes, int parallelism
    ) {
        final int div = Math.max(writes.size() / 40, 1);
        final boolean mod = writes.size() % div == 0;

        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();

        final AsyncFuture<CollectedTimes> results =
            async.eventuallyCollect(writes, new StreamCollector<Times, CollectedTimes>() {
                final ConcurrentLinkedQueue<Long> runTimes = new ConcurrentLinkedQueue<>();
                final ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<>();

                @Override
                public void resolved(Times result) throws Exception {
                    runTimes.add(result.getRuntime());
                    executionTimes.addAll(result.getExecutionTimes());
                    check();
                }

                @Override
                public void failed(Throwable cause) throws Exception {
                    cause.printStackTrace(out);
                    errors.incrementAndGet();
                    check();
                }

                @Override
                public void cancelled() throws Exception {
                    errors.incrementAndGet();
                    check();
                }

                private void check() {
                    if (count.incrementAndGet() % div == 0) {
                        dot();
                    }
                }

                private void dot() {
                    out.print(errors.getAndSet(0) > 0 ? '!' : '.');
                    out.flush();
                }

                @Override
                public CollectedTimes end(int resolved, int failed, int cancelled)
                    throws Exception {
                    if (!mod) {
                        dot();
                    }

                    out.println();
                    out.flush();

                    final Histogram.Builder runTimes = Histogram.builder(this.runTimes);
                    final Histogram.Builder executionTimes = Histogram.builder(this.executionTimes);
                    return new CollectedTimes(runTimes, executionTimes, errors.get());
                }
            }, parallelism);

        return results;
    }

    private List<Series> generateSeries(int count) {
        final List<Series> series = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            series.add(Series.of("generated",
                ImmutableMap.of("type", "generated", "generated-id", Integer.toString(i))));
        }

        return series;
    }

    private long average(List<Long> times) {
        long total = 0;

        for (final long v : times) {
            total += v;
        }

        return total / times.size();
    }

    @ToString
    public static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--limit",
            usage = "Maximum number of datapoints to fetch (default: 1000000)", metaVar = "<int>")
        private int limit = 1000000;

        @Option(name = "--series", usage = "Number of different series to write",
            metaVar = "<number>")
        private int series = 10;

        @Option(name = "--generator", usage = "Generator to use", metaVar = "<generator>")
        private String generator = "random";

        @Option(name = "--target", usage = "Group to write data to", metaVar = "<backend>")
        private List<String> targets = new ArrayList<>();

        @Option(name = "--history", usage = "Seconds of data to copy (default: 3600)",
            metaVar = "<number>")
        private Duration history = Duration.of(3600, TimeUnit.SECONDS);

        @Option(name = "--writes", usage = "How many writes to perform (default: 1000)",
            metaVar = "<number>")
        private int writes = 10;

        @Option(name = "--parallelism",
            usage = "The number of requests to send in parallel (default: 100)",
            metaVar = "<number>")
        private int parallelism = 100;

        @Option(name = "--warmup", usage = "Loop the test several times to warm up",
            metaVar = "<number>")
        private int warmup = 4;

        @Option(name = "--loop", usage = "Loop the test several times", metaVar = "<number>")
        private int loop = 8;
    }

    @Data
    private static final class CollectedTimes {
        private final Histogram.Builder runTimes;
        private final Histogram.Builder executionTimes;
        private final int errors;
    }

    @Data
    private static final class Times {
        private final List<Long> executionTimes;
        private final long runtime;
    }

    public static WritePerformance setup(final CoreComponent core) {
        return DaggerWritePerformance_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        WritePerformance task();
    }
}
