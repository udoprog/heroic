package com.spotify.heroic.analytics;

import com.spotify.heroic.metric.MetricBackend;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

public class NullMetricAnalytics implements MetricAnalytics {
    private final AsyncFramework async;

    public NullMetricAnalytics(final AsyncFramework async) {
        this.async = async;
    }

    @Override
    public MetricBackend wrap(MetricBackend backend) {
        return backend;
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.resolved();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }
}
