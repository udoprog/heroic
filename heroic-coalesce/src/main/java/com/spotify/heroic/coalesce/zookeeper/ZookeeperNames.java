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

import lombok.Data;

@Data
public class ZookeeperNames {
    public static final String TASKS = "tasks";
    public static final String DISCOVERY = "discovery";
    public static final String COORDINATOR = "coordinator";

    public static String tasks() {
        return "/" + TASKS;
    }

    public static Node node(final String id) {
        return new Node(id);
    }

    public static String discovery() {
        return "/" + DISCOVERY;
    }

    public static String coordinator() {
        return "/" + COORDINATOR;
    }

    @Data
    public static class Node {
        private final String id;

        public String tasks() {
            return "/" + TASKS + "/" + id;
        }

        public String task(String taskId) {
            return tasks() + "/" + taskId;
        }
    }
}
