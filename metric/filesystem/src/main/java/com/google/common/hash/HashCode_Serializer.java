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

package com.google.common.hash;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import java.io.IOException;

public class HashCode_Serializer implements Serializer<HashCode> {
    private final Serializer<byte[]> byteArray;

    public HashCode_Serializer(final SerializerFramework serializer) {
        this.byteArray = serializer.byteArray();
    }

    @Override
    public void serialize(
        final SerialWriter buffer, final HashCode value
    ) throws IOException {
        byteArray.serialize(buffer, value.asBytes());
    }

    @Override
    public HashCode deserialize(final SerialReader buffer) throws IOException {
        return HashCode.fromBytesNoCopy(byteArray.deserialize(buffer));
    }
}
