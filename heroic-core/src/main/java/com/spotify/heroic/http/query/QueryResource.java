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

package com.spotify.heroic.http.query;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.http.CoreHttpContextFactory;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.querylogging.QueryContextFactory;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Path("query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    private final JavaxRestFramework httpAsync;
    private final QueryManager query;
    private final AsyncFramework async;
    private final QueryLogger queryLogger;

    @Inject
    public QueryResource(
        final JavaxRestFramework httpAsync, final QueryManager query, final AsyncFramework async,
        final QueryLoggerFactory queryLoggerFactory
    ) {
        this.httpAsync = httpAsync;
        this.query = query;
        this.async = async;
        this.queryLogger = queryLoggerFactory.create("QueryResource");
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.TEXT_PLAIN)
    public void metricsText(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context final HttpServletRequest servletReq, final String query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryContext queryContext = QueryContextFactory.create(Optional.empty());
        queryLogger.logHttpQueryText(queryContext, query, httpContext);

        final Query q = this.query.newQueryFromString(query).build();

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q, queryContext);

        bindMetricsResponse(response, callback, queryContext);
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context final HttpServletRequest servletReq, final QueryMetrics query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryContext queryContext = QueryContextFactory.create(query.getClientContext());
        queryLogger.logHttpQueryJson(queryContext, query, httpContext);

        final Query q = query.toQueryBuilder(this.query::newQueryFromString).build();

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q, queryContext);

        bindMetricsResponse(response, callback, queryContext);
    }

    @POST
    @Path("batch")
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("backend") String group,
        @Context final HttpServletRequest servletReq, final QueryBatch query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));

        final List<AsyncFuture<Pair<String, QueryResult>>> futures = new ArrayList<>();
        final Map<String, QueryContext> queryContexts = new HashMap<>();

        query.getQueries().ifPresent(queries -> {
            for (final Map.Entry<String, QueryMetrics> e : queries.entrySet()) {
                final String queryKey = e.getKey();
                final QueryMetrics qm = e.getValue();
                final Query q = qm
                    .toQueryBuilder(this.query::newQueryFromString)
                    .rangeIfAbsent(query.getRange())
                    .build();

                final QueryContext queryContext = QueryContextFactory.create(qm.getClientContext());
                queryLogger.logHttpQueryJson(queryContext, qm, httpContext);

                futures.add(g.query(q, queryContext).directTransform(r -> Pair.of(queryKey, r)));
                queryContexts.put(queryKey, queryContext);
            }
        });

        final AsyncFuture<QueryBatchResponse> future =
            async.collect(futures).directTransform(entries -> {
                final ImmutableMap.Builder<String, QueryMetricsResponse> results =
                    ImmutableMap.builder();

                for (final Pair<String, QueryResult> e : entries) {
                    final String queryKey = e.getLeft();
                    final QueryResult r = e.getRight();
                    final QueryContext queryContext = queryContexts.get(queryKey);
                    final QueryMetricsResponse qmr =
                        new QueryMetricsResponse(queryContext.getQueryId(), r.getRange(),
                            r.getGroups(), r.getErrors(), r.getTrace(), r.getLimits());

                    if (queryContext != null) {
                        queryLogger.logFinalResponse(queryContext, qmr);
                    }

                    results.put(queryKey, qmr);
                }

                return new QueryBatchResponse(results.build());
            });

        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, future);
    }

    private void bindMetricsResponse(
        final AsyncResponse response, final AsyncFuture<QueryResult> callback,
        final QueryContext queryContext
    ) {
        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, callback, r -> {
            QueryMetricsResponse qmr =
                new QueryMetricsResponse(queryContext.getQueryId(), r.getRange(), r.getGroups(),
                    r.getErrors(), r.getTrace(), r.getLimits());
            queryLogger.logFinalResponse(queryContext, qmr);
            return qmr;
        });
    }

    @Data
    public static final class StreamId {
        private final Map<String, String> tags;
        private final UUID id;
    }

    @Data
    private static final class StreamQuery {
        private final QueryManager.Group group;
        private final Query query;
    }
}
