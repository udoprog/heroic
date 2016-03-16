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
import com.spotify.heroic.grammar.FunctionExpression;

import java.util.function.Function;

public interface AggregationRegistry {
    /**
     * Register a new aggregation.
     *
     * @param id The id of the new aggregation, will be used in the type field, and in the DSL.
     * @param type The type of the aggregation.
     * @param dsl DSL factory for the aggregation.
     */
    <A extends Aggregation> void register(
        String id, Class<A> type, Function<AggregationArguments, Aggregation> dsl
    );

    void registerAlias(String id, Function<FunctionExpression, FunctionExpression> dsl);

    Module module();

    /**
     * Create an AggregationFactory instance using this Registry.
     *
     * Usage of this instance must be thread-safe in relationship with the AggregationRegistry.
     * Modifications done to the AggregationRegistry must be reflected in the AggregationFactory at
     * all times.
     *
     * @return A new AggregationFactory instance.
     */
    AggregationFactory newAggregationFactory();
}
