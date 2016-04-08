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

package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BiFunctionAggregation;
import com.spotify.heroic.grammar.Expression;
import lombok.EqualsAndHashCode;

import java.util.Optional;

@EqualsAndHashCode(callSuper = true, of = {"NAME", "defaultValue"})
public class Divide extends BiFunctionAggregation {
    public static final String NAME = "divide";

    private final double defaultValue;

    @Override
    protected double applyPoint(final double a, final double b) {
        if (b == 0D) {
            return defaultValue;
        }

        return a / b;
    }

    @JsonCreator
    public Divide(
        @JsonProperty("left") Optional<Expression> left,
        @JsonProperty("right") Optional<Expression> right,
        @JsonProperty("default") Optional<Double> defaultValue
    ) {
        super(left, right);
        this.defaultValue = defaultValue.orElse(0D);
    }

    @JsonProperty("default")
    public double getDefaultValue() {
        return defaultValue;
    }
}
