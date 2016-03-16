package com.spotify.heroic.aggregation;

import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.metric.MetricCollection;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AggregationUtils {
    static AsyncFramework async = TinyAsync.builder().build();

    static RunResult run(
        final Aggregation a, List<AggregationState> states, DateRange range, Duration duration
    ) throws Exception {
        final AggregationContext ctx =
            AggregationContext.of(async, states, range, duration, Function.identity());

        final AggregationContext out = a.setup(ctx).get();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Queue<MetricCollection> queue = new ConcurrentLinkedQueue<>();

        Observable
            .chain(out.input().stream().map(s -> s.getObservable()).collect(Collectors.toList()))
            .observe(new Observer<MetricCollection>() {
                @Override
                public void observe(final MetricCollection value) throws Exception {
                    queue.add(value);
                }

                @Override
                public void fail(final Throwable cause) throws Exception {
                    error.set(cause);
                }

                @Override
                public void end() throws Exception {
                    latch.countDown();
                }
            });

        latch.await();

        return new RunResult(Optional.ofNullable(error.get()), new ArrayList<>(queue));
    }

    @Data
    static class RunResult {
        final Optional<Throwable> error;
        final List<MetricCollection> results;
    }
}
