package com.spotify.heroic.analytics.bigtable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.bigtable.BigtableConnection;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;

public class BigtableMetricAnalytics implements MetricAnalytics {
    final Managed<BigtableConnection> connection;
    final AsyncFramework async;
    final String tableName;
    final ObjectMapper mapper;

    public BigtableMetricAnalytics(final Managed<BigtableConnection> connection,
            final AsyncFramework async, final String tableName, final ObjectMapper mapper) {
        this.connection = connection;
        this.async = async;
        this.tableName = tableName;
        this.mapper = mapper;
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(c -> {
            return async.resolved();
        });
    }

    @Override
    public MetricBackend wrap(final MetricBackend backend) {
        return new BigtableAnalyticsMetricBackend(connection, tableName, mapper, async, backend);
    }
}
