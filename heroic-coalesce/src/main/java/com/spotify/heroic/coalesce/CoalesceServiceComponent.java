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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.ServiceComponent;
import com.spotify.heroic.cluster.NodeMetadata;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class CoalesceServiceComponent
        implements ServiceComponent, CoalesceConnection.SessionChange {
    private final String id;
    private final AsyncFramework async;
    private final Managed<CoalesceConnection> connection;
    private final CoalescePersistence persistence;
    private final ScheduledExecutorService scheduler;
    private final String node;

    private final Managed<ServiceState> state;

    private volatile ScheduledFuture<?> coordinateSchedule = null;

    @Inject
    public CoalesceServiceComponent(@Named("id") final String id, @Named("node") final String node,
            final AsyncFramework async, final Managed<CoalesceConnection> connection,
            final CoalescePersistence persistence, final ScheduledExecutorService scheduler,
            final NodeMetadata metadata) {
        this.id = id;
        this.async = async;
        this.connection = connection;
        this.persistence = persistence;
        this.scheduler = scheduler;
        this.node = node;

        this.state = async.managed(new ManagedSetup<ServiceState>() {
            @Override
            public AsyncFuture<ServiceState> construct() throws Exception {
                final Borrowed<CoalesceConnection> b = connection.borrow();
                final CoalesceConnection c = b.get();

                try {
                    return c.session(node, CoalesceServiceComponent.this).directTransform(s -> {
                        final CoalesceCoordinator coordinator =
                                new CoalesceCoordinator(async, s, persistence);
                        return new ServiceState(b, s, coordinator);
                    });
                } catch (final Exception e) {
                    b.release();
                    throw e;
                }
            }

            @Override
            public AsyncFuture<Void> destruct(final ServiceState state) throws Exception {
                return state.s.close().onFinished(() -> {
                    state.c.release();
                });
            }
        });
    }

    @Override
    public boolean isReady() {
        return connection.isReady() && state.isReady();
    }

    @Override
    public void isCoordinator() {
        log.info("Becoming coordinator");
    }

    @Override
    public void notCoordinator() {
        log.info("Loosing coordinator");
    }

    @Override
    public void taskAdded(String id) {
        log.info("Add task: " + id);
    }

    @Override
    public void taskRemoved(String id) {
        log.info("Remove task: " + id);
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start().lazyTransform(n -> state.start());
    }

    @Override
    public AsyncFuture<Void> stop() {
        return state.stop().lazyTransform(n -> connection.stop());
    }

    public AsyncFuture<Set<String>> getTaskIds() {
        return persistence.getTaskIds();
    }

    public AsyncFuture<Void> addTask(CoalesceTask task) {
        return persistence.addTask(task);
    }

    public AsyncFuture<Void> deleteTask(String id) {
        return persistence.deleteTask(id);
    }

    public AsyncFuture<Optional<CoalesceTask>> getTask(String id) {
        return persistence.getTask(id);
    }

    @Data
    public static class NodeInfo {
        private final String node;
        private final Set<String> tasks;
        private final long nextUpdate;

        public NodeInfo withAddedTasks(final Iterable<String> tasks) {
            final Set<String> newTasks =
                    ImmutableSet.copyOf(Sets.union(this.tasks, ImmutableSet.copyOf(tasks)));
            return new NodeInfo(node, newTasks, nextUpdate);
        }

        public NodeInfo withDeletedTasks(final Iterable<String> tasks) {
            final Set<String> newTasks =
                    ImmutableSet.copyOf(Sets.difference(this.tasks, ImmutableSet.copyOf(tasks)));
            return new NodeInfo(node, newTasks, nextUpdate);
        }
    }

    @RequiredArgsConstructor
    public static class ServiceState {
        private final Borrowed<CoalesceConnection> c;
        private final CoalesceConnection.Session s;
        private final CoalesceCoordinator coordinator;
    }
}
