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

package com.spotify.heroic.coalesce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.spotify.heroic.ServiceComponent;
import com.spotify.heroic.common.JavaxRestFramework;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("coalesce")
public class CoalesceResource {
    final AsyncFramework async;
    final Map<String, CoalesceServiceComponent> components;
    final JavaxRestFramework rest;

    @Inject
    public CoalesceResource(final AsyncFramework async,
            final Set<ServiceComponent> serviceComponents, final JavaxRestFramework rest) {
        this.async = async;
        this.components = buildComponents(serviceComponents);
        this.rest = rest;
    }

    private Map<String, CoalesceServiceComponent> buildComponents(
            final Set<ServiceComponent> input) {
        final ImmutableMap.Builder<String, CoalesceServiceComponent> components =
                ImmutableMap.builder();

        input.stream().filter(CoalesceServiceComponent.class::isInstance)
                .map(CoalesceServiceComponent.class::cast).forEach(c -> {
                    components.put(c.getId(), c);
                });

        return components.build();
    }

    @GET
    @Path("{id}/tasks")
    public void getTaskIds(@Suspended AsyncResponse response, @PathParam("id") String id) {
        final List<AsyncFuture<Set<String>>> results =
                ImmutableList.copyOf(components(id).map(c -> c.getTaskIds()).iterator());
        rest.bind(response, async.collect(results)
                .directTransform(all -> ImmutableSet.copyOf(Iterables.concat(all))));
    }

    @GET
    @Path("{id}/tasks/{taskId}")
    public void getTask(@Suspended AsyncResponse response, @PathParam("id") String id,
            @PathParam("taskId") String taskId) {
        final List<AsyncFuture<Optional<CoalesceTask>>> futures =
                ImmutableList.copyOf(Iterators.concat(components(id)
                        .map(c -> ids(taskId).map(c::getTask).iterator()).iterator()));

        rest.bind(response, async.collect(futures).directTransform(all -> {
            final ImmutableList.Builder<CoalesceTask> results = ImmutableList.builder();

            all.forEach(g -> {
                g.ifPresent(results::add);
            });

            return results.build();
        }));
    }

    @POST
    @Path("{id}/tasks")
    public void addTask(@Suspended AsyncResponse response, @PathParam("id") String id,
            CoalesceTask task) {
        checkNotNull(task, "task body is required");

        final List<AsyncFuture<Void>> results =
                ImmutableList.copyOf(components(id).map(c -> c.addTask(task)).iterator());
        rest.bind(response, async.collectAndDiscard(results).directTransform(v -> "OK"));
    }

    @DELETE
    @Path("{id}/tasks/{taskId}")
    public void addTask(@Suspended AsyncResponse response, @PathParam("id") String id,
            @PathParam("taskId") String taskId) {
        final List<AsyncFuture<Void>> results = ImmutableList.copyOf(Iterators.concat(
                components(id).map(c -> ids(taskId).map(c::deleteTask).iterator()).iterator()));

        rest.bind(response, async.collectAndDiscard(results).directTransform(v -> "OK"));
    }

    private static final Splitter PATH_SPLITTER = Splitter.on(",").trimResults();

    private Stream<String> ids(String id) {
        final Stream.Builder<String> stream = Stream.builder();
        PATH_SPLITTER.split(id).forEach(stream::add);
        return stream.build();
    }

    private Stream<CoalesceServiceComponent> components(String id) {
        final Stream.Builder<CoalesceServiceComponent> stream = Stream.builder();

        PATH_SPLITTER.split(id).forEach(p -> {
            final CoalesceServiceComponent c = components.get(id);

            if (c != null) {
                stream.add(c);
            }
        });

        return stream.build();
    }
}
