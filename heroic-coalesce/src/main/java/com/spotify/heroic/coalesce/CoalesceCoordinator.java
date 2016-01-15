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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.async.ExclusiveTask;
import com.spotify.heroic.coalesce.CoalesceServiceComponent.NodeInfo;
import com.spotify.heroic.common.Duration;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoalesceCoordinator extends ExclusiveTask {
    /* ten second random drift for refreshing node information */
    public static final long RANDOM_DRIFT = 10000;

    private final AsyncFramework async;
    private final CoalesceConnection.Session session;
    private final CoalescePersistence persistence;

    private final Duration nodeRefresh;

    private final Random random = new Random();
    private final ConcurrentHashMap<String, NodeInfo> info = new ConcurrentHashMap<>();

    public CoalesceCoordinator(final AsyncFramework async, final CoalesceConnection.Session session,
            final CoalescePersistence persistence) {
        super("coordinator");

        this.async = async;
        this.session = session;
        this.persistence = persistence;

        this.nodeRefresh = Duration.of(5, TimeUnit.MINUTES);
    }

    @Override
    public AsyncFuture<Void> invoke() throws Exception {
        final List<AsyncFuture<Void>> maintenance = new ArrayList<>();

        maintenance.add(discoverNewNodes());
        maintenance.add(refreshNodesIfNeeded());

        return async.collectAndDiscard(maintenance).lazyTransform(n -> {
            final List<NodeInfo> nodes = ImmutableList.copyOf(info.values());

            if (nodes.isEmpty()) {
                return async.resolved();
            }

            return persistence.getTaskIds().lazyTransform(ids -> {
                final Set<String> all = new HashSet<>(ids);
                final Set<String> assign = new HashSet<>(ids);

                final List<Pair<NodeInfo, Set<String>>> unassign = new ArrayList<>();

                for (final NodeInfo node : nodes) {
                    final Set<String> assigned = new HashSet<>(node.getTasks());
                    assigned.removeAll(all);

                    assign.removeAll(node.getTasks());

                    if (!assigned.isEmpty()) {
                        unassign.add(Pair.of(node, assigned));
                    }
                }

                final List<AsyncFuture<Void>> shuffles = new ArrayList<>();

                if (assign.size() > 0) {
                    log.info("Tasks that must be distributed: " + assign);

                    /* optimal number of tasks assigned (ish) */
                    final int optimal = (ids.size() / nodes.size()) + 1;
                    shuffles.addAll(assignTasks(nodes, optimal, assign));
                }

                if (!unassign.isEmpty()) {
                    log.info("Tasks that must be unassigned: " + unassign);

                    shuffles.addAll(unassignTasks(unassign));
                }

                return async.collectAndDiscard(shuffles);
            });
        });
    }

    private AsyncFuture<Void> discoverNewNodes() {
        return session.findNodes().lazyTransform(nodes -> {
            final List<AsyncFuture<NodeInfo>> futures = new ArrayList<>();

            for (final String n : nodes) {
                if (info.containsKey(n)) {
                    continue;
                }

                futures.add(refreshNode(n));
            }

            return async.collect(futures).directTransform(update -> {
                for (final NodeInfo n : update) {
                    info.put(n.getNode(), n);
                }

                return null;
            });
        });
    }

    private AsyncFuture<NodeInfo> refreshNode(final String n) throws Exception {
        return session.findTasks(n).directTransform(tasks -> {
            final Set<String> activeTasks = ImmutableSet.copyOf(tasks);

            return new NodeInfo(n, activeTasks, System.currentTimeMillis()
                    + nodeRefresh.toMilliseconds() + (long) (RANDOM_DRIFT * random.nextDouble()));
        });
    }

    private AsyncFuture<Void> refreshNodesIfNeeded() throws Exception {
        final long now = System.currentTimeMillis();

        final List<NodeInfo> nodesToRefresh = ImmutableList
                .copyOf(info.values().stream().filter(n -> n.getNextUpdate() < now).iterator());

        final List<AsyncFuture<NodeInfo>> futures = new ArrayList<>();

        if (nodesToRefresh.isEmpty()) {
            return async.resolved();
        }

        for (final NodeInfo n : nodesToRefresh) {
            futures.add(refreshNode(n.getNode()).catchFailed(error -> {
                log.error("Failed to refresh node: " + n + ", using cached", error);
                return n;
            }));
        }

        return async.collect(futures).directTransform(update -> {
            for (final NodeInfo n : update) {
                info.put(n.getNode(), n);
            }

            return null;
        });
    }

    private List<AsyncFuture<Void>> unassignTasks(final List<Pair<NodeInfo, Set<String>>> unassign)
            throws Exception {
        final List<AsyncFuture<Void>> shuffles = new ArrayList<>();

        for (final Pair<NodeInfo, Set<String>> u : unassign) {
            final NodeInfo node = u.getLeft();

            final List<AsyncFuture<List<String>>> localDeletes = new ArrayList<>();

            for (final String taskId : u.getRight()) {
                localDeletes.add(session.removeTask(node.getNode(), taskId)
                        .directTransform(n -> taskId).<List<String>> directTransform(v -> {
                            log.info("Deleted task {} from {}", taskId, node);
                            return ImmutableList.of(taskId);
                        }).catchFailed(error -> {
                            log.error("Failed to delete task {} from {}", taskId, node);
                            return ImmutableList.of();
                        }));
            }

            shuffles.add(async.collect(localDeletes).directTransform(tasks -> {
                if (!tasks.isEmpty()) {
                    info.put(node.getNode(), node.withDeletedTasks(Iterables.concat(tasks)));
                }

                return null;
            }));
        }

        return shuffles;
    }

    private List<AsyncFuture<Void>> assignTasks(final List<NodeInfo> nodes, final int optimal,
            final Set<String> assign) throws Exception {
        final List<AsyncFuture<Void>> shuffles = new ArrayList<>();

        final LinkedList<String> queue = new LinkedList<>(assign);

        for (final NodeInfo node : nodes) {
            if (queue.isEmpty()) {
                break;
            }

            int diff = (optimal - node.getTasks().size());

            final List<AsyncFuture<List<String>>> nodeAssigned = new ArrayList<>();

            while (!queue.isEmpty() && diff-- > 0) {
                final String taskId = queue.pop();

                nodeAssigned.add(session.registerTask(node.getNode(), taskId)
                        .<List<String>> directTransform(v -> {
                            log.info("Assigned task {} to {}", taskId, node);
                            return ImmutableList.of(taskId);
                        }).catchFailed(error -> {
                            log.error("Failed to assign task {} to {}", taskId, node);
                            return ImmutableList.of();
                        }));
            }

            shuffles.add(async.collect(nodeAssigned).directTransform(tasks -> {
                if (!tasks.isEmpty()) {
                    info.put(node.getNode(), node.withAddedTasks(Iterables.concat(tasks)));
                }

                return null;
            }));
        }

        assert queue.isEmpty();
        return shuffles;
    }
}
