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

package com.spotify.heroic.grammar;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@ValueName("list")
@Data
public final class ListValue implements Value {
    private final List<Value> list;

    @Override
    public Value sub(Value other) {
        throw new IllegalArgumentException("list does not support subtraction");
    }

    @Override
    public Value add(Value other) {
        final ListValue o = other.cast(this);
        final ArrayList<Value> list = new ArrayList<Value>();
        list.addAll(this.list);
        list.addAll(o.list);
        return new ListValue(list);
    }

    public String toString() {
        return "[" + list + "]";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof ListValue) {
            return (T) this;
        }

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(ListValue.class)) {
            return (T) this;
        }

        throw new ValueTypeCastException(this, to);
    }
}
