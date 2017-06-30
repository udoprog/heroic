/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic;

import com.google.common.hash.Hasher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Data;

@Data
public class ObjectHasher {
    private static final int START_OBJECT = 1;
    private static final int END_OBJECT = 2;

    private static final int START_LIST = 3;
    private static final int END_LIST = 4;

    private static final int START_SET = 5;
    private static final int END_SET = 6;

    private static final int FIELD = 99;

    private static final int INTEGER = 100;
    private static final int LONG = 101;
    private static final int DOUBLE = 102;
    private static final int STRING = 103;
    private static final int BOOLEAN = 104;
    private static final int ENUM = 105;

    private static final int OPTIONAL = 90;
    private static final int OPTIONAL_PRESENT = 50;
    private static final int OPTIONAL_ABSENT = 51;

    private final Hasher hasher;

    public <T> void putField(
        final String name, final T value, final Consumer<T> hashTo
    ) {
        hasher.putInt(FIELD);
        putString(name);
        hashTo.accept(value);
    }

    public void putObject(final Class<?> cls) {
        putObject(cls, () -> {
        });
    }

    public void putObject(final Class<?> cls, final Runnable runnable) {
        startObject(cls);
        runnable.run();
        endObject();
    }

    private void startObject(final Class<?> cls) {
        hasher.putInt(START_OBJECT);
        putString(cls.getCanonicalName());
    }

    private void endObject() {
        hasher.putInt(END_OBJECT);
    }

    public void putString(final String name) {
        hasher.putInt(STRING);
        hasher.putInt(name.length());
        hasher.putString(name, StandardCharsets.UTF_8);
    }

    public void putLong(final long value) {
        hasher.putInt(LONG);
        hasher.putLong(value);
    }

    public void putDouble(final double value) {
        hasher.putInt(DOUBLE);
        hasher.putDouble(value);
    }

    public void putBoolean(final boolean value) {
        hasher.putInt(BOOLEAN);
        hasher.putBoolean(value);
    }

    public <T> void putOptional(
        final Optional<T> optional, final BiConsumer<T, ObjectHasher> hashTo
    ) {
        hasher.putInt(OPTIONAL);

        if (optional.isPresent()) {
            hasher.putInt(OPTIONAL_PRESENT);
            hashTo.accept(optional.get(), this);
        } else {
            hasher.putInt(OPTIONAL_ABSENT);
        }
    }

    public <T> Consumer<T> with(final BiConsumer<T, ObjectHasher> inner) {
        return value -> inner.accept(value, this);
    }

    public <T extends Enum> Consumer<T> enumValue() {
        return value -> {
            hasher.putInt(ENUM);
            putString(value.getClass().getCanonicalName());
            putString(value.name());
        };
    }

    public <T> Consumer<List<T>> list(final Consumer<T> hashTo) {
        return list -> {
            hasher.putInt(START_LIST);

            for (final T val : list) {
                hashTo.accept(val);
            }

            hasher.putInt(END_LIST);
        };
    }

    public <T> Consumer<Optional<T>> optional(final Consumer<T> hashTo) {
        return optional -> {
            hasher.putInt(OPTIONAL);

            if (optional.isPresent()) {
                hasher.putInt(OPTIONAL_PRESENT);
                hashTo.accept(optional.get());
            } else {
                hasher.putInt(OPTIONAL_ABSENT);
            }
        };
    }

    public Consumer<String> string() {
        return string -> {
            hasher.putInt(STRING);
            hasher.putInt(string.length());
            hasher.putString(string, StandardCharsets.UTF_8);
        };
    }

    public Consumer<Double> doubleValue() {
        return value -> {
            hasher.putInt(DOUBLE);
            hasher.putDouble(value);
        };
    }

    public Consumer<Integer> integer() {
        return value -> {
            hasher.putInt(INTEGER);
            hasher.putInt(value);
        };
    }

    public Consumer<Long> longValue() {
        return value -> {
            hasher.putInt(LONG);
            hasher.putLong(value);
        };
    }

    public Consumer<Boolean> bool() {
        return value -> {
            hasher.putInt(BOOLEAN);
            hasher.putBoolean(value);
        };
    }

    public <T> Consumer<SortedSet<T>> sortedSet(final Consumer<T> inner) {
        return values -> {
            hasher.putInt(START_SET);

            for (final T value : values) {
                inner.accept(value);
            }

            hasher.putInt(END_SET);
        };
    }
}
