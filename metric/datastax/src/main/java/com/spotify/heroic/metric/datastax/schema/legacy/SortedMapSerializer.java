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

package com.spotify.heroic.metric.datastax.schema.legacy;

import com.spotify.heroic.metric.datastax.TypeSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@RequiredArgsConstructor
public class SortedMapSerializer<A, B> implements TypeSerializer<SortedMap<A, B>> {
    private final TypeSerializer<A> a;
    private final TypeSerializer<B> b;

    @Override
    public ByteBuffer serialize(final SortedMap<A, B> value) {
        final List<Pair<ByteBuffer, ByteBuffer>> buffers = new ArrayList<>();

        short size = 0;

        for (final Map.Entry<A, B> e : value.entrySet()) {
            final ByteBuffer key = a.serialize(e.getKey());
            final ByteBuffer val = b.serialize(e.getValue());

            size += key.limit() + val.limit();

            buffers.add(Pair.of(key, val));
        }

        final ByteBuffer buffer = ByteBuffer.allocate(4 + 8 * value.size() + size);
        buffer.putShort((short) buffers.size());

        for (final Pair<ByteBuffer, ByteBuffer> p : buffers) {
            buffer.putShort((short) p.getLeft().remaining());
            buffer.put(p.getLeft());
            buffer.putShort((short) p.getRight().remaining());
            buffer.put(p.getRight());
        }

        buffer.flip();
        return buffer;
    }

    @Override
    public SortedMap<A, B> deserialize(ByteBuffer buffer) {
        final short len = buffer.getShort();

        final SortedMap<A, B> map = new TreeMap<>();

        for (short i = 0; i < len; i++) {
            final A key = next(buffer, a);
            final B value = next(buffer, b);

            if (value == null) {
                continue;
            }

            map.put(key, value);
        }

        return map;
    }

    private <T> T next(ByteBuffer buffer, TypeSerializer<T> serializer) {
        final short segment = buffer.getShort();
        final ByteBuffer slice = buffer.slice();
        slice.limit(segment);
        final T value = serializer.deserialize(slice);

        buffer.position(buffer.position() + segment);
        return value;
    }
}
