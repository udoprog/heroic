package com.spotify.heroic.analytics.bigtable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.analytics.AnalyticsModule;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.BigtableConnectionBuilder;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import javax.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;

public class BigtableAnalyticsModule implements AnalyticsModule {
    private final String project;
    private final String zone;
    private final String cluster;
    private final CredentialsBuilder credentials;

    @JsonCreator
    public BigtableAnalyticsModule(@JsonProperty("project") String project,
            @JsonProperty("zone") String zone, @JsonProperty("cluster") String cluster,
            @JsonProperty("credentials") CredentialsBuilder credentials) {
        this.project = project;
        this.zone = zone;
        this.cluster = cluster;
        this.credentials = credentials;
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public Managed<BigtableConnection> connection(final AsyncFramework async,
                    final ExecutorService executorService) {
                return async.managed(new ManagedSetup<BigtableConnection>() {
                    @Override
                    public AsyncFuture<BigtableConnection> construct() throws Exception {
                        return async.call(new BigtableConnectionBuilder(project, zone, cluster,
                                credentials, async, executorService));
                    }

                    @Override
                    public AsyncFuture<Void> destruct(final BigtableConnection value)
                            throws Exception {
                        return async.call(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                value.close();
                                return null;
                            }
                        });
                    }
                });
            }

            @Override
            protected void configure() {
                bind(MetricAnalytics.class).to(BigtableMetricAnalytics.class);
            }
        };
    }
}
