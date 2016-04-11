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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.spotify.heroic.grammar.FunctionExpression;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Serializes aggregation configurations.
 * <p>
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation
 * as a prefixed short.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class CoreAggregationRegistry implements AggregationRegistry {
    final Serializer<String> string;

    final Map<Class<?>, String> definitionMap = new HashMap<>();

    final Map<String, Function<FunctionExpression, FunctionExpression>> aliasMap =
        new ConcurrentHashMap<>();
    final Map<String, Function<AggregationArguments, Aggregation>> builderMap =
        new ConcurrentHashMap<>();

    private final Object lock = new Object();

    @Override
    public void registerAlias(
        final String id, final Function<FunctionExpression, FunctionExpression> alias
    ) {
        synchronized (lock) {
            if (aliasMap.containsKey(id) || builderMap.containsKey(id)) {
                throw new IllegalArgumentException(
                    "An aggregation or alias with the same id (" + id +
                        ") is already registered");
            }

            aliasMap.put(id, alias);
        }
    }

    @Override
    public <A extends Aggregation> void register(
        final String id, final Class<A> type,
        final Function<AggregationArguments, Aggregation> builder
    ) {
        synchronized (lock) {
            if (definitionMap.containsKey(type)) {
                throw new IllegalArgumentException(
                    "An aggregation with the same type (" + type.getCanonicalName() +
                        ") is already registered");
            }

            if (builderMap.containsKey(id)) {
                throw new IllegalArgumentException("An aggregation with the same id (" + id +
                    ") is already registered");
            }

            definitionMap.put(type, id);
            builderMap.put(id, builder);
        }
    }

    public Module module() {
        synchronized (lock) {
            final SimpleModule m = new SimpleModule("aggregationRegistry");

            for (final Map.Entry<Class<?>, String> e : definitionMap.entrySet()) {
                m.registerSubtypes(new NamedType(e.getKey(), e.getValue()));
            }

            return m;
        }
    }

    /* Sharing of aliasMap and builderMap is essential, but it is (more or less) guaranteed that
     * neither is modified after the loading state. */
    @Override
    public AggregationFactory newAggregationFactory() {
        return new CoreAggregationFactory(aliasMap, builderMap);
    }
}
