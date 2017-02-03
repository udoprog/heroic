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

package com.spotify.heroic.metric.filesystem;

import com.spotify.heroic.metric.filesystem.transaction.InMemoryState;
import com.spotify.heroic.metric.filesystem.transaction.WriteEvents;
import com.spotify.heroic.metric.filesystem.transaction.WritePoints;
import com.spotify.heroic.metric.filesystem.transaction.WriteState;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@AutoSerialize.SubTypes(
    {@AutoSerialize.SubType(WritePoints.class), @AutoSerialize.SubType(WriteEvents.class)})
public interface Transaction {
    void write(WriteState state, long txId) throws Exception;

    /**
     * Apply the given transaction against the given in-memory state.
     */
    void writeInMemory(InMemoryState state);

    /**
     * Undo the current transaction against the given in-memory state.
     */
    void undoInMemory(InMemoryState state) throws Exception;
}
