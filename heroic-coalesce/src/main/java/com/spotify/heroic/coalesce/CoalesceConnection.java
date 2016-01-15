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

import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface CoalesceConnection {
    AsyncFuture<Session> session(String node, SessionChange change) throws Exception;

    AsyncFuture<Void> close();

    AsyncFuture<Void> start();

    interface SessionChange {
        void isCoordinator();

        void notCoordinator();

        void taskAdded(final String id);

        void taskRemoved(final String id);
    }

    interface Session {
        AsyncFuture<List<String>> findTasks(String node) throws Exception;

        AsyncFuture<Void> registerTask(String node, String taskId) throws Exception;

        AsyncFuture<Void> removeTask(String node, String taskId) throws Exception;

        AsyncFuture<Void> registerNode(String node);

        AsyncFuture<Void> unregisterNode(String node);

        AsyncFuture<List<String>> findNodes();

        AsyncFuture<Void> deleteAllTasks(String node) throws Exception;

        public AsyncFuture<Void> close();
    }
}
