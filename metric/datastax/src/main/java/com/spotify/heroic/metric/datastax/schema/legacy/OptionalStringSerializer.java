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

import com.google.common.base.Charsets;
import com.spotify.heroic.metric.datastax.TypeSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public class OptionalStringSerializer implements TypeSerializer<Optional<String>> {
    private static final String EMPTY = "";
    private static final Charset UTF8 = Charsets.UTF_8;

    private static final int IS_EMPTY = 0x0;
    private static final int IS_EMPTY_STRING = 0x1;
    private static final int IS_STRING = 0x2;

    @Override
    public ByteBuffer serialize(Optional<String> value) {
        if (!value.isPresent()) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_EMPTY);
            b.flip();
            return b;
        }

        final String v = value.get();

        if (v.isEmpty()) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_EMPTY_STRING);
            b.flip();
            return b;
        }

        final byte[] bytes = v.getBytes(UTF8);
        final ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);
        buffer.put((byte) IS_STRING);
        buffer.put(bytes);

        buffer.flip();
        return buffer;
    }

    @Override
    public Optional<String> deserialize(ByteBuffer buffer) {
        final byte flag = buffer.get();

        if (flag == IS_EMPTY) {
            return Optional.empty();
        }

        if (flag == IS_EMPTY_STRING) {
            return Optional.of(EMPTY);
        }

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Optional.of(new String(bytes, UTF8));
    }
}
