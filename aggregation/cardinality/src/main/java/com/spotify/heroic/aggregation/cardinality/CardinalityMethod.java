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

package com.spotify.heroic.aggregation.cardinality;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.StringExpression;
import lombok.Data;

import java.beans.ConstructorProperties;
import java.util.Optional;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(CardinalityMethod.HyperLogLogCardinalityMethod.class),
    @JsonSubTypes.Type(CardinalityMethod.ExactCardinalityMethod.class)
})
public interface CardinalityMethod {
    CardinalityBucket build(final long timestamp);

    default CardinalityMethod reducer() {
        throw new RuntimeException("reducer not supported");
    }

    @Data
    @JsonTypeName("exact")
    class ExactCardinalityMethod implements CardinalityMethod {
        @ConstructorProperties({})
        public ExactCardinalityMethod() {
        }

        @Override
        public CardinalityBucket build(final long timestamp) {
            return new ExactCardinalityBucket(timestamp);
        }
    }

    @Data
    @JsonTypeName("hyperloglog")
    class HyperLogLogCardinalityMethod implements CardinalityMethod {
        public static double DEFAULT_PRECISION = 0.01;

        private final double precision;

        @ConstructorProperties({"precision"})
        public HyperLogLogCardinalityMethod(Optional<Double> precision) {
            this.precision = precision.orElse(DEFAULT_PRECISION);
        }

        @Override
        public CardinalityBucket build(final long timestamp) {
            return new HyperLogLogCardinalityBucket(timestamp, precision);
        }

        @Override
        public CardinalityMethod reducer() {
            return new ReduceHyperLogLogCardinalityMethod();
        }
    }

    @Data
    class ReduceHyperLogLogCardinalityMethod implements CardinalityMethod {
        @Override
        public CardinalityBucket build(final long timestamp) {
            return new ReduceHyperLogLogCardinalityBucket(timestamp);
        }
    }

    static CardinalityMethod fromExpression(Expression expression) {
        return expression.visit(new Expression.Visitor<CardinalityMethod>() {
            @Override
            public CardinalityMethod visitString(final StringExpression e) {
                switch (e.getString()) {
                    case "exact":
                        return new HyperLogLogCardinalityMethod(Optional.empty());
                    case "hyperloglog":
                        return new HyperLogLogCardinalityMethod(Optional.empty());
                    default:
                        throw e.getContext().error("Unknown cardinality method");
                }
            }

            @Override
            public CardinalityMethod defaultAction(final Expression e) {
                throw e.getContext().error("Unsupported method");
            }
        });
    }
}
