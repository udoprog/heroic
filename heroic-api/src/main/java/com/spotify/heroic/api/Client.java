package com.spotify.heroic.api;

import com.spotify.heroic.proto.heroic.QueryMetrics;
import java.util.concurrent.CompletableFuture;

public interface Client {
    CompletableFuture<QueryMetrics.Response> queryMetrics(final QueryMetrics query);
}
