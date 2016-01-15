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

package com.spotify.heroic.coalesce.zookeeper;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.coalesce.CoalesceConnection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Data;

@Data
public class ZookeeperCoalesceConnection implements CoalesceConnection {
    private final AsyncFramework async;
    private final CuratorFramework client;

    private static final Splitter PATH_SPLITTER = Splitter.on("/");

    @Override
    public AsyncFuture<CoalesceConnection.Session> session(final String node,
            final CoalesceConnection.SessionChange callback) throws Exception {
        final ZookeeperNames.Node names = ZookeeperNames.node(node);

        final LeaderLatch coordinator = new LeaderLatch(client, ZookeeperNames.coordinator());

        final ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class)
                .client(client).basePath(ZookeeperNames.discovery()).build();

        final TreeCache tasksTree =
                TreeCache.newBuilder(client, names.tasks()).setCacheData(false).build();

        final LeaderLatchListener coordinatorListener = new LeaderLatchListener() {
            @Override
            public void notLeader() {
                callback.notCoordinator();
            }

            @Override
            public void isLeader() {
                callback.isCoordinator();
            }
        };

        coordinator.addListener(coordinatorListener);

        final TreeCacheListener tasksListener = new TreeCacheListener() {
            @Override
            public void childEvent(final CuratorFramework c, final TreeCacheEvent e)
                    throws Exception {
                switch (e.getType()) {
                case NODE_ADDED:
                    if (names.tasks().equals(e.getData().getPath())) {
                        break;
                    }

                    callback.taskAdded(getId(e));
                    break;
                case NODE_REMOVED:
                    if (names.tasks().equals(e.getData().getPath())) {
                        return;
                    }

                    callback.taskRemoved(getId(e));
                    break;
                default:
                    break;
                }
            }

            private String getId(final TreeCacheEvent e) {
                final List<String> parts = PATH_SPLITTER.splitToList(e.getData().getPath());
                final String id = parts.get(parts.size() - 1);
                return id;
            }
        };

        tasksTree.getListenable().addListener(tasksListener);

        final List<AsyncFuture<Void>> starters = new ArrayList<>();

        starters.add(async.call(() -> {
            coordinator.start();
            return null;
        }));

        starters.add(async.call(() -> {
            discovery.start();
            return null;
        }));

        starters.add(async.call(() -> {
            tasksTree.start();
            return null;
        }));

        return async.collectAndDiscard(starters).directTransform(n2 -> {
            return new Session(coordinatorListener, tasksListener, coordinator, discovery,
                    tasksTree, names);
        });
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.call(() -> {
            client.start();
            return null;
        });
    }

    @Override
    public AsyncFuture<Void> close() {
        return async.call(() -> {
            client.close();
            return null;
        });
    }

    @Data
    private class Session implements CoalesceConnection.Session {
        private final LeaderLatchListener coordinatorListener;
        private final TreeCacheListener tasksListener;

        private final LeaderLatch coordinator;
        private final ServiceDiscovery<String> discovery;
        private final TreeCache tasksTree;
        private final ZookeeperNames.Node names;

        @Override
        public AsyncFuture<Void> registerTask(final String node, final String id) throws Exception {
            return curatorCreateTask(node, id).directTransform(event -> {
                final KeeperException.Code code = KeeperException.Code.get(event.getResultCode());

                if (code == KeeperException.Code.OK || code == KeeperException.Code.NODEEXISTS) {
                    return null;
                }

                throw new RuntimeException(
                        "Create on " + event.getPath() + " failed with resultCode: " + code);
            });
        }

        @Override
        public AsyncFuture<Void> removeTask(final String node, final String id) throws Exception {
            return curatorDeleteTask(node, id).directTransform(event -> {
                final KeeperException.Code code = KeeperException.Code.get(event.getResultCode());

                if (code == KeeperException.Code.OK || code == KeeperException.Code.NONODE) {
                    return null;
                }

                throw new RuntimeException(
                        "Remove on " + event.getPath() + " failed with resultCode: " + code);
            });
        }

        @Override
        public AsyncFuture<Void> deleteAllTasks(String node) throws Exception {
            return findTasks(node).lazyTransform(tasks -> {
                final ImmutableList.Builder<AsyncFuture<Void>> futures = ImmutableList.builder();

                for (final String taskId : tasks) {
                    futures.add(removeTask(node, taskId));
                }

                return async.collectAndDiscard(futures.build());
            });
        }

        @Override
        public AsyncFuture<List<String>> findTasks(final String node) throws Exception {
            return curatorFindTasks(node).directTransform(event -> {
                final KeeperException.Code code = KeeperException.Code.get(event.getResultCode());

                if (code == KeeperException.Code.NONODE) {
                    return ImmutableList.of();
                }

                if (code == KeeperException.Code.OK) {
                    return event.getChildren();
                }

                throw new RuntimeException(
                        "Get chilhdren on " + event.getPath() + " failed with resultCode: " + code);
            });
        }

        @Override
        public AsyncFuture<Void> registerNode(final String node) {
            return async.call(() -> {
                final ServiceInstance<String> instance =
                        ServiceInstance.<String> builder().id(node).name("workers").build();
                discovery.registerService(instance);
                return null;
            });
        }

        @Override
        public AsyncFuture<Void> unregisterNode(final String node) {
            return async.call(() -> {
                final ServiceInstance<String> instance =
                        ServiceInstance.<String> builder().id(node).name("workers").build();
                discovery.unregisterService(instance);
                return null;
            });
        }

        @Override
        public AsyncFuture<List<String>> findNodes() {
            return async.call(() -> {
                final ImmutableList.Builder<String> nodes = ImmutableList.builder();

                for (final ServiceInstance<String> instance : discovery
                        .queryForInstances("workers")) {
                    nodes.add(instance.getId());
                }

                return nodes.build();
            });
        }

        @Override
        public AsyncFuture<Void> close() {
            final List<AsyncFuture<Void>> closers = new ArrayList<>();

            closers.add(async.call(() -> {
                coordinator.close();
                return null;
            }));

            closers.add(async.call(() -> {
                discovery.close();
                return null;
            }));

            closers.add(async.call(() -> {
                tasksTree.close();
                return null;
            }));

            closers.add(async.call(() -> {
                coordinator.removeListener(coordinatorListener);
                return null;
            }));

            closers.add(async.call(() -> {
                tasksTree.getListenable().removeListener(tasksListener);
                return null;
            }));

            return async.collectAndDiscard(closers);
        }

        private AsyncFuture<CuratorEvent> curatorDeleteTask(String node, final String taskId)
                throws Exception {
            final ResolvableFuture<CuratorEvent> future = async.future();

            client.delete().inBackground((client, event) -> future.resolve(event))
                    .forPath(ZookeeperNames.node(node).task(taskId));

            return future;
        }

        private AsyncFuture<CuratorEvent> curatorFindTasks(final String node) throws Exception {
            final ResolvableFuture<CuratorEvent> future = async.future();

            client.getChildren().inBackground((client, event) -> future.resolve(event))
                    .forPath(ZookeeperNames.node(node).tasks());

            return future;
        }

        private AsyncFuture<CuratorEvent> curatorCreateTask(final String node, final String taskId)
                throws Exception {
            final ResolvableFuture<CuratorEvent> future = async.future();

            client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                    .inBackground((client, event) -> future.resolve(event))
                    .forPath(ZookeeperNames.node(node).task(taskId));

            return future;
        }
    }
}
