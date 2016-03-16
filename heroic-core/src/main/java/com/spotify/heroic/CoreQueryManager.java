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

package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.LetExpression;
import com.spotify.heroic.grammar.QueryExpression;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.RangeExpression;
import com.spotify.heroic.grammar.ReferenceExpression;
import com.spotify.heroic.grammar.Statements;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CoreQueryManager implements QueryManager {
    private final Set<String> features;
    private final AsyncFramework async;
    private final ClusterManager cluster;
    private final AggregationFactory aggregations;
    private final FilterFactory filters;
    private final QueryParser parser;
    private final QueryCache queryCache;

    @Inject
    public CoreQueryManager(
        @Named("features") final Set<String> features, final AsyncFramework async,
        final ClusterManager cluster, final AggregationFactory aggregations,
        final FilterFactory filters, final QueryParser parser, final QueryCache queryCache
    ) {
        this.features = features;
        this.async = async;
        this.cluster = cluster;
        this.aggregations = aggregations;
        this.filters = filters;
        this.parser = parser;
        this.queryCache = queryCache;
    }

    @Override
    public CoreQueryManagerGroup useGroup(String group) {
        return newGroup(cluster.useGroup(group));
    }

    @Override
    public Collection<CoreQueryManagerGroup> useGroupPerNode(String group) {
        final List<CoreQueryManagerGroup> result = new ArrayList<>();

        for (ClusterNode.Group g : cluster.useGroup(group)) {
            result.add(newGroup(ImmutableList.of(g)));
        }

        return result;
    }

    @Override
    public CoreQueryManagerGroup useDefaultGroup() {
        return newGroup(cluster.useDefaultGroup());
    }

    @Override
    public Collection<CoreQueryManagerGroup> useDefaultGroupPerNode() {
        final List<CoreQueryManagerGroup> result = new ArrayList<>();

        for (ClusterNode.Group g : cluster.useDefaultGroup()) {
            result.add(newGroup(ImmutableList.of(g)));
        }

        return result;
    }

    @Override
    public QueryBuilder newQuery() {
        return new QueryBuilder(filters);
    }

    @Override
    public QueryBuilder newQueryFromString(final String queryString) {
        final Statements parsed = parser.parse(queryString);

        final Map<String, Query> statements = new HashMap<>();

        parsed
            .getExpressions()
            .forEach(e -> e.visit(new Expression.Visitor<Optional<LetExpression>>() {
                @Override
                public Optional<LetExpression> visitLet(final LetExpression e) {
                    return Optional.of(e);
                }

                @Override
                public Optional<LetExpression> defaultAction(final Expression e) {
                    return Optional.empty();
                }
            }).ifPresent(let -> {
                statements.put(let.getReference().getName(),
                    queryBuilderFromExpression(let.getExpression()).build());
            }));

        final List<Expression> queries = parsed
            .getExpressions()
            .stream()
            .filter(e -> !(e instanceof LetExpression))
            .collect(Collectors.toList());

        if (queries.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one non-let expression");
        }

        return queryBuilderFromExpression(queries.get(0)).statements(Optional.of(statements));
    }

    @Override
    public String queryToString(final Query q) {
        return parser.stringifyQuery(q);
    }

    @Override
    public String queryToString(final Query q, final Optional<Integer> indent) {
        return parser.stringifyQuery(q, indent);
    }

    @Override
    public AsyncFuture<Void> initialized() {
        return cluster.initialized();
    }

    private CoreQueryManagerGroup newGroup(final Iterable<ClusterNode.Group> groups) {
        final long now = System.currentTimeMillis();

        return new CoreQueryManagerGroup(async, filters, aggregations, queryCache, features, now,
            this::queryBuilderFromExpression, groups);
    }

    private QueryBuilder queryBuilderFromExpression(final Expression q) {
        return q.visit(new Expression.Visitor<QueryBuilder>() {
            @Override
            public QueryBuilder visitQuery(final QueryExpression e) {
                /* get aggregation that is part of statement, if any */
                final Optional<Aggregation> aggregation = e
                    .getSelect()
                    .flatMap(a -> a.visit(new Expression.Visitor<Optional<Aggregation>>() {
                        // ignore references since they will be picked up later.
                        @Override
                        public Optional<Aggregation> visitReference(final ReferenceExpression e) {
                            return Optional.empty();
                        }

                        @Override
                        public Optional<Aggregation> defaultAction(final Expression e) {
                            final Optional<Aggregation> agg = aggregations.fromExpression(a);

                            if (!agg.isPresent()) {
                                throw new IllegalArgumentException("Expected aggregation: " + a);
                            }

                            return agg;
                        }
                    }));

                final Optional<String> reference =
                    e.getSelect().flatMap(a -> a.visit(new Expression.Visitor<Optional<String>>() {
                        @Override
                        public Optional<String> visitReference(final ReferenceExpression e) {
                            return Optional.of(e.getName());
                        }

                        @Override
                        public Optional<String> defaultAction(final Expression e) {
                            return Optional.empty();
                        }
                    }));

                final Optional<QueryDateRange> range =
                    e.getRange().map(r -> r.visit(new Expression.Visitor<QueryDateRange>() {
                        @Override
                        public QueryDateRange visitRange(final RangeExpression e) {
                            return new QueryDateRange.Expression(e);
                        }
                    }));

                return newQuery()
                    .source(e.getSource())
                    .range(range)
                    .aggregation(aggregation)
                    .reference(reference)
                    .filter(e.getFilter());
            }
        });
    }
}
