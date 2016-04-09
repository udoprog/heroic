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

package com.spotify.heroic.suggest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.metric.NodeError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@Data
public class TagValueSuggest {
    public static final List<NodeError> EMPTY_ERRORS = new ArrayList<NodeError>();
    public static final List<String> EMPTY_VALUES = new ArrayList<String>();

    private final List<NodeError> errors;
    private final List<String> values;
    private final boolean limited;

    @JsonCreator
    public TagValueSuggest(
        @JsonProperty("errors") List<NodeError> errors,
        @JsonProperty("values") List<String> values, @JsonProperty("limited") Boolean limited
    ) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.values = Optional.fromNullable(values).or(EMPTY_VALUES);
        this.limited = Optional.fromNullable(limited).or(false);
    }

    public TagValueSuggest(List<String> values, boolean limited) {
        this(EMPTY_ERRORS, values, limited);
    }

    public static Collector<TagValueSuggest, TagValueSuggest> reduce(final int limit) {
        return new Collector<TagValueSuggest, TagValueSuggest>() {
            @Override
            public TagValueSuggest collect(Collection<TagValueSuggest> groups) throws Exception {
                final List<NodeError> errors = new ArrayList<>();
                final SortedSet<String> values = new TreeSet<>();

                boolean limited = false;

                for (final TagValueSuggest g : groups) {
                    errors.addAll(g.errors);
                    values.addAll(g.values);
                    limited = limited || g.limited;
                }

                limited = limited || values.size() >= limit;
                return new TagValueSuggest(errors,
                    ImmutableList.copyOf(values).subList(0, Math.min(values.size(), limit)),
                    limited);
            }
        };
    }

    public static Transform<Throwable, ? extends TagValueSuggest> nodeError(
        final NodeRegistryEntry node
    ) {
        return new Transform<Throwable, TagValueSuggest>() {
            @Override
            public TagValueSuggest transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagValueSuggest(ImmutableList.<NodeError>of(
                    NodeError.fromThrowable(m.getId(), c.toString(), m.getTags(), e)), EMPTY_VALUES,
                    false);
            }
        };
    }

    public static Transform<Throwable, ? extends TagValueSuggest> nodeError(
        final ClusterNode.Group group
    ) {
        return new Transform<Throwable, TagValueSuggest>() {
            @Override
            public TagValueSuggest transform(Throwable e) throws Exception {
                final List<NodeError> errors =
                    ImmutableList.<NodeError>of(NodeError.fromThrowable(group.node(), e));
                return new TagValueSuggest(errors, EMPTY_VALUES, false);
            }
        };
    }
}
