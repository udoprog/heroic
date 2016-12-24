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

import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.SortedMap;

public class MetricsRowKeySerializer implements TypeSerializer<MetricsRowKey> {
    private final TypeSerializer<Optional<String>> key = new OptionalStringSerializer();
    private final TypeSerializer<SortedMap<String, String>> tags =
        new SortedMapSerializer<>(new StringSerializer(), new StringSerializer());
    private final TypeSerializer<Long> longNumber = new LongSerializer();

    private static final TypeSerializer<ByteBuffer> BB = new TypeSerializer<ByteBuffer>() {
        @Override
        public ByteBuffer serialize(final ByteBuffer value) {
            return value;
        }

        @Override
        public ByteBuffer deserialize(final ByteBuffer buffer) {
            return buffer;
        }
    };

    @Override
    public ByteBuffer serialize(MetricsRowKey value) {
        final CompositeComposer composer = new CompositeComposer();

        {
            final CompositeComposer keyTags = new CompositeComposer();
            keyTags.add(value.getKey(), this.key);
            keyTags.add(value.getTags(), this.tags);
            composer.add(keyTags.serialize(), BB);
        }

        composer.add(value.getBase(), this.longNumber);
        return composer.serialize();
    }

    @Override
    public MetricsRowKey deserialize(ByteBuffer buffer) {
        final CompositeStream reader = new CompositeStream(buffer);

        final Optional<String> key;
        final SortedMap<String, String> tags;

        {
            final CompositeStream keyTags = new CompositeStream(reader.next(BB));
            key = keyTags.next(this.key);
            tags = keyTags.next(this.tags);
        }

        final long base = reader.next(this.longNumber);
        return new MetricsRowKey(key, tags, base);
    }
}
