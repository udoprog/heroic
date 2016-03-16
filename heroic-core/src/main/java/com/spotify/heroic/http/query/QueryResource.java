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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Path("query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    public static final String TEXT_GRAPHVIZ = "text/vnd.graphviz";
    public static final String COMPACT_TRACING = "compactTracing";

    private final JavaxRestFramework httpAsync;
    private final QueryManager query;
    private final AsyncFramework async;

    @Inject
    public QueryResource(JavaxRestFramework httpAsync, QueryManager query, AsyncFramework async) {
        this.httpAsync = httpAsync;
        this.query = query;
        this.async = async;
    }

    private final Cache<UUID, StreamQuery> streamQueries =
        CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).<UUID, StreamQuery>build();

    @POST
    @Path("metrics/stream")
    public List<StreamId> metricsStream(
        @Context UriInfo uri, @QueryParam("backend") String backendGroup, QueryMetrics query
    ) {
        final Query request = setupQuery(query, uri).build();

        final Collection<? extends QueryManager.Group> groups =
            this.query.useGroupPerNode(backendGroup);
        final List<StreamId> ids = new ArrayList<>();

        for (QueryManager.Group group : groups) {
            final UUID id = UUID.randomUUID();
            streamQueries.put(id, new StreamQuery(group, request));
            ids.add(new StreamId(group.first().node().metadata().getTags(), id));
        }

        return ids;
    }

    @POST
    @Path("metrics/stream/{id}")
    public void metricsStreamId(
        @Suspended final AsyncResponse response, @PathParam("id") final UUID id
    ) {
        if (id == null) {
            throw new BadRequestException("Id must be a valid UUID");
        }

        final StreamQuery streamQuery = streamQueries.getIfPresent(id);

        if (streamQuery == null) {
            throw new NotFoundException("Stream query not found with id: " + id);
        }

        final Query q = streamQuery.getQuery();
        final QueryManager.Group group = streamQuery.getGroup();
        final AsyncFuture<QueryResult> callback = group.query(q);

        callback.onResolved(r -> streamQueries.invalidate(id));

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("analyze")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(TEXT_GRAPHVIZ)
    public void analyzeJsonToGraphviz(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context UriInfo uri, QueryMetrics input
    ) {
        bindAnalyzeGraphviz(response, query.useGroup(group), setupQuery(input, uri).build());
    }

    @POST
    @Path("analyze")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void analyzeJsonToJson(
        @Suspended final AsyncResponse response, @Context UriInfo uri,
        @QueryParam("group") String group, QueryMetrics input
    ) {
        httpAsync.bind(response, query.useGroup(group).analyze(setupQuery(input, uri).build()));
    }

    @POST
    @Path("analyze")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(TEXT_GRAPHVIZ)
    public void analyzeTextToGraphviz(
        @Suspended final AsyncResponse response, @Context UriInfo uri,
        @QueryParam("group") String group, String input
    ) {
        bindAnalyzeGraphviz(response, query.useGroup(group), setupQuery(input, uri).build());
    }

    @POST
    @Path("analyze")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public void analyzeTextToJson(
        @Suspended final AsyncResponse response, @Context UriInfo uri,
        @QueryParam("group") String group, String input
    ) {
        httpAsync.bind(response, query.useGroup(group).analyze(setupQuery(input, uri).build()));
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public void metricsText(
        @Suspended final AsyncResponse response, @Context UriInfo uri,
        @QueryParam("group") String group, String input
    ) {
        bindMetricsResponse(response, query.useGroup(group).query(setupQuery(input, uri).build()));
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context UriInfo uri, QueryMetrics input
    ) {
        bindMetricsResponse(response, query.useGroup(group).query(setupQuery(input, uri).build()));
    }

    @POST
    @Path("batch")
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("backend") String backendGroup,
        @Context UriInfo uri, final QueryBatch query
    ) {
        final QueryManager.Group group = this.query.useGroup(backendGroup);

        final List<AsyncFuture<Pair<String, QueryResult>>> futures = new ArrayList<>();

        for (final Map.Entry<String, QueryMetrics> e : query.getQueries().entrySet()) {
            final Query q = setupQuery(e.getValue(), uri).rangeIfAbsent(query.getRange()).build();
            futures.add(group.query(q).directTransform(r -> Pair.of(e.getKey(), r)));
        }

        final AsyncFuture<QueryBatchResponse> future =
            async.collect(futures).directTransform(entries -> {
                final ImmutableMap.Builder<String, QueryMetricsResponse> results =
                    ImmutableMap.builder();

                for (final Pair<String, QueryResult> e : entries) {
                    final QueryResult r = e.getRight();
                    results.put(e.getLeft(),
                        new QueryMetricsResponse(r.getData(), Duration.empty(), r.getErrors(),
                            r.getTrace()));
                }

                return new QueryBatchResponse(results.build());
            });

        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, future);
    }

    private void bindMetricsResponse(
        final AsyncResponse response, final AsyncFuture<QueryResult> callback
    ) {
        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, callback,
            r -> new QueryMetricsResponse(r.getData(), Duration.empty(), r.getErrors(),
                r.getTrace()));
    }

    private void bindAnalyzeGraphviz(
        final AsyncResponse response, final QueryManager.Group g, final Query q
    ) {
        final AsyncFuture<String> callback = g.analyze(q).directTransform(r -> {
            final StringWriter writer = new StringWriter();

            try (final PrintWriter out = new PrintWriter(writer)) {
                r.writeGraphviz(out);
            }

            return writer.toString();
        });

        httpAsync.bind(response, callback);
    }

    @SuppressWarnings("deprecation")
    private QueryBuilder setupQuery(final QueryMetrics q, final UriInfo uri) {
        final Supplier<? extends QueryBuilder> supplier = () -> query
            .newQuery()
            .key(q.getKey())
            .tags(q.getTags())
            .groupBy(q.getGroupBy())
            .filter(q.getFilter())
            .range(q.getRange())
            .aggregation(q.getAggregation())
            .source(q.getSource())
            .options(q.getOptions());

        return modifyBuilderWithUri(q
            .getQuery()
            .map(query::newQueryFromString)
            .orElseGet(supplier)
            .rangeIfAbsent(q.getRange())
            .optionsIfAbsent(q.getOptions())
            .features(q.getFeatures()), uri);
    }

    private QueryBuilder setupQuery(final String q, final UriInfo uri) {
        return modifyBuilderWithUri(query.newQueryFromString(q), uri);
    }

    private QueryBuilder modifyBuilderWithUri(final QueryBuilder builder, final UriInfo uri) {
        final Optional<Boolean> compactTracing = Optional
            .ofNullable(uri.getQueryParameters().getFirst(COMPACT_TRACING))
            .map(Boolean::parseBoolean);

        return builder.modifyOptions(
            o -> compactTracing.map(c -> o.withCompactTracing(c)).orElse(o));
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
