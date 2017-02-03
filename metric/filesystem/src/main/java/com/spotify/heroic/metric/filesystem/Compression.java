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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent_Serializer;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint_Serializer;
import com.spotify.heroic.metric.filesystem.segmentio.BasicSegmentEncoding;
import com.spotify.heroic.metric.filesystem.segmentio.GorillaSegmentEncoding;
import com.spotify.heroic.metric.filesystem.segmentio.SegmentEncoding;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

public enum Compression {
    NONE, GORILLA;

    @JsonCreator
    public static Compression create(final String value) {
        return valueOf(value.toUpperCase());
    }

    @JsonValue
    public String value() {
        return name().toLowerCase();
    }

    public SegmentEncoding newSegmentIO(final SerializerFramework serializer) {
        final Serializer<SegmentHeader> segmentHeader = new SegmentHeader_Serializer(serializer);
        final Serializer<SegmentPoint> points = new SegmentPoint_Serializer(serializer);
        final Serializer<SegmentEvent> events = new SegmentEvent_Serializer(serializer);

        final BasicSegmentEncoding basicSegmentIO =
            new BasicSegmentEncoding(serializer, segmentHeader, points, events);

        switch (this) {
            case GORILLA:
                return new GorillaSegmentEncoding(basicSegmentIO);
            default:
            case NONE:
                return basicSegmentIO;
        }
    }
}
