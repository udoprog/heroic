package com.spotify.heroic.analytics.bigtable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.api.Row;

import java.util.Collection;
import java.util.List;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
class BigtableAnalyticsMetricBackend implements MetricBackend {
    private final Managed<BigtableConnection> connection;
    private final String tableName;
    private final ObjectMapper mapper;
    private final AsyncFramework async;
    private final MetricBackend backend;

    @Override
    public boolean isReady() {
        return backend.isReady();
    }

    @Override
    public Groups getGroups() {
        return backend.getGroups();
    }

    @Override
    public Statistics getStatistics() {
        return backend.getStatistics();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return backend.configure();
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        return backend.write(write);
    }

    @Override
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes) {
        return backend.write(writes);
    }

    @Override
    public AsyncFuture<FetchData> fetch(MetricType type, Series series, DateRange range,
            FetchQuotaWatcher watcher, QueryOptions options) {
        reportFetch(series);
        return backend.fetch(type, series, range, watcher, options);
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return backend.listEntries();
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeys(BackendKeyFilter filter,
            QueryOptions options) {
        return backend.streamKeys(filter, options);
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeysPaged(BackendKeyFilter filter,
            QueryOptions options, int pageSize) {
        return backend.streamKeysPaged(filter, options, pageSize);
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        return backend.serializeKeyToHex(key);
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return backend.deserializeKeyFromHex(key);
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        return backend.deleteKey(key, options);
    }

    @Override
    public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
        return backend.countKey(key, options);
    }

    @Override
    public AsyncFuture<MetricCollection> fetchRow(BackendKey key) {
        return backend.fetchRow(key);
    }

    @Override
    public AsyncObservable<MetricCollection> streamRow(BackendKey key) {
        return backend.streamRow(key);
    }

    private void reportFetch(Series series) {
        final Borrowed<BigtableConnection> b = connection.borrow();

        if (!b.isValid()) {
            return;
        }

        try {
            final ByteString key = ByteString.copyFrom(mapper.writeValueAsBytes(series));

            final BigtableConnection c = b.get();

            final AsyncFuture<Row> request = c.client().readModifyWriteRow(tableName,
                    key, c.client().readModifyWriteRules().increment(1L).build());

            request.onFailed(e -> {
                log.error("Failed to increment counter {}", series, e);
            }).onFinished(b::releasing);
        } catch (final Exception e) {
            log.error("Failed to increment counter", e);
            return;
        } finally {
            b.release();
        }
    }
}
