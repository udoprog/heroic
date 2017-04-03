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

package com.spotify.heroic.http.cluster;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.http.DataResponse;
import com.spotify.heroic.server.Response;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.net.URI;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("cluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private final AsyncFramework async;
    private final ClusterManager cluster;

    @Inject
    public ClusterResource(final AsyncFramework async, final ClusterManager cluster) {
        this.async = async;
        this.cluster = cluster;
    }

    /**
     * Encode/Decode functions, helpful when interacting with cassandra through cqlsh.
     */
    @GET
    @Path("status")
    public AsyncFuture<Response> status() {
        final List<ClusterNodeStatus> nodes = convert(cluster.getNodes());
        final ClusterStatus status = new ClusterStatus(nodes, cluster.getStatistics());
        return async.resolved(Response.ok(status));
    }

    private List<ClusterNodeStatus> convert(List<ClusterNode> nodes) {
        return ImmutableList.copyOf(nodes.stream().map(this::convert).iterator());
    }

    private ClusterNodeStatus convert(ClusterNode node) {
        final NodeMetadata m = node.metadata();

        return new ClusterNodeStatus(node.toString(), m.getId(), m.getVersion(), m.getTags());
    }

    @GET
    @Path("nodes")
    public AsyncFuture<Set<URI>> getNodes(URI uri) {
        return cluster.getStaticNodes();
    }

    @DELETE
    @Path("nodes")
    public AsyncFuture<DataResponse<Boolean>> removeNode(URI uri) {
        return cluster.removeStaticNode(uri).directTransform(ignore -> new DataResponse<>(true));
    }

    @POST
    @Path("nodes")
    public AsyncFuture<DataResponse<Boolean>> addNode(URI uri) {
        return cluster.addStaticNode(uri).directTransform(ignore -> new DataResponse<>(true));
    }

    @POST
    @Path("refresh")
    public AsyncFuture<Void> refresh() {
        return cluster.refresh();
    }
}
