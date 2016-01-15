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

package com.spotify.heroic.coalesce.persistence;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.spotify.heroic.coalesce.CoalescePersistence;
import com.spotify.heroic.coalesce.CoalesceTask;
import com.spotify.heroic.coalesce.tasks.HttpPingCoalesceTask;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@JsonTypeName("fake")
public class FakePersistence implements CoalescePersistence {
    private final Map<String, CoalesceTask> tasks = new HashMap<>();

    {
        tasks.put("http-ping-1",
                new HttpPingCoalesceTask("http-ping-1", "1", "https://google.com"));
        tasks.put("http-ping-2",
                new HttpPingCoalesceTask("http-ping-2", "1", "https://google.com"));
    }

    private final AsyncFramework async;

    @Inject
    public FakePersistence(AsyncFramework async) {
        this.async = async;
    }

    @Override
    public AsyncFuture<Optional<CoalesceTask>> getTask(final String id) {
        return async.resolved(Optional.ofNullable(tasks.get(id)));
    }

    @Override
    public AsyncFuture<Set<String>> getTaskIds() {
        return async.resolved(ImmutableSet.copyOf(tasks.keySet()));
    }

    @Override
    public AsyncFuture<Void> addTask(CoalesceTask task) {
        tasks.put(task.getId(), task);
        return async.resolved();
    }


    @Override
    public AsyncFuture<Void> deleteTask(String id) {
        tasks.remove(id);
        return async.resolved();
    }
}
