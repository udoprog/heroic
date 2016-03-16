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

import lombok.RequiredArgsConstructor;

public interface AggregationLookup {
    <T> T visit(Visitor<T> visitor);

    boolean isExpression();

    @RequiredArgsConstructor
    class Context implements AggregationLookup {
        private final LookupFunction context;

        @Override
        public <T> T visit(final Visitor<T> visitor) {
            return visitor.visitContext(context);
        }

        @Override
        public boolean isExpression() {
            return false;
        }
    }

    @RequiredArgsConstructor
    class Expression implements AggregationLookup {
        private final com.spotify.heroic.grammar.Expression expression;

        @Override
        public <T> T visit(final Visitor<T> visitor) {
            return visitor.visitExpression(expression);
        }

        @Override
        public boolean isExpression() {
            return true;
        }
    }

    interface Visitor<T> {
        T visitContext(LookupFunction context);

        T visitExpression(com.spotify.heroic.grammar.Expression e);
    }
}
