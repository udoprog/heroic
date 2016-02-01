package com.spotify.heroic.analytics;

import com.google.inject.Module;
import com.google.inject.PrivateModule;

public class NullAnalyticsModule implements AnalyticsModule {
    public Module module() {
        return new PrivateModule() {
            @Override
            protected void configure() {
                bind(MetricAnalytics.class).to(NullMetricAnalytics.class);
                expose(MetricAnalytics.class);
            }
        };
    }
}
