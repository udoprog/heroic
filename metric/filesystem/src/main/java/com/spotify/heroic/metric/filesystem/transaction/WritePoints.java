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

package com.spotify.heroic.metric.filesystem.transaction;

import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.filesystem.SegmentKey;
import com.spotify.heroic.metric.filesystem.Transaction;
import eu.toolchain.serializer.AutoSerialize;
import java.util.List;
import java.util.NavigableMap;
import lombok.Data;

@AutoSerialize
@Data
public class WritePoints extends WriteTransaction implements Transaction {
    private final SegmentKey key;
    private final List<Point> points;

    @Override
    protected SegmentKey key() {
        return key;
    }

    @Override
    public void write(final WriteState state, final long txId) throws Exception {
        final NavigableMap<Long, Double> map = getOrCreateWriteMap(state.points, txId);

        if (map == null) {
            return;
        }

        for (final Point p : points) {
            map.put(p.getTimestamp(), p.getValue());
        }
    }

    @Override
    public void writeInMemory(final InMemoryState state) {
        final NavigableMap<Long, Double> map = state.points.data;

        for (final Point p : points) {
            map.put(p.getTimestamp(), p.getValue());
        }
    }

    @Override
    public void undoInMemory(final InMemoryState state) throws Exception {
        final NavigableMap<Long, Double> map = state.points.data;

        for (final Point p : points) {
            map.remove(p.getTimestamp(), p.getValue());
        }
    }
}
