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
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.ExpressionEvaluator;
import com.spotify.heroic.grammar.LetExpression;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.Statements;
import eu.toolchain.async.AsyncFramework;

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
    private final ExpressionEvaluator expressions;

    @Inject
    public CoreQueryManager(
        @Named("features") final Set<String> features, final AsyncFramework async,
        final ClusterManager cluster, final AggregationFactory aggregations,
        final FilterFactory filters, final QueryParser parser, final QueryCache queryCache,
        final ExpressionEvaluator expressions
    ) {
        this.features = features;
        this.async = async;
        this.cluster = cluster;
        this.aggregations = aggregations;
        this.filters = filters;
        this.parser = parser;
        this.queryCache = queryCache;
        this.expressions = expressions;
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
    public QueryInstanceGroup newQueryFromString(final String queryString) {
        final Statements parsed = parser.parse(queryString);

        final Map<String, QueryInstance> statements = new HashMap<>();

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
                    expressions.evaluateQuery(let.getExpression()));
            }));

        final List<Expression> queries = parsed
            .getExpressions()
            .stream()
            .filter(e -> !(e instanceof LetExpression))
            .collect(Collectors.toList());

        return new QueryInstanceGroup(queries.stream().map(q -> {
            return expressions.evaluateQuery(q).withStatements(statements);
        }).collect(Collectors.toList()));
    }

    private CoreQueryManagerGroup newGroup(final Iterable<ClusterNode.Group> groups) {
        return new CoreQueryManagerGroup(async, filters, aggregations, queryCache, features,
            expressions::evaluateQuery, groups);
    }
}
