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

package com.spotify.heroic.filter;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface FilterEncoding<T> {
    interface Decoder {
        /**
         * read next item as a string.
         */
        Optional<String> string() throws IOException;

        /**
         * read next item as a filter.
         */
        Optional<Filter> filter() throws IOException;

        /**
         * read next item as an entry.
         */
        Optional<Map.Entry<String, String>> entry() throws IOException;
    }

    interface Encoder {
        /**
         * Serialize next item as a string.
         */
        void string(String string) throws IOException;

        /**
         * Serialize next item as a filter.
         */
        void filter(Filter filter) throws IOException;

        /**
         * Serialize next item as an entry.
         */
        void entry(Map.Entry<String, String> entry) throws IOException;
    }

    T deserialize(Decoder decoder) throws IOException;

    void serialize(Encoder encoder, T filter) throws IOException;

    FilterEncodingComponent<String> STRING =
        new FilterEncodingComponent<>(Decoder::string, Encoder::string);
    FilterEncodingComponent<Filter> FILTER =
        new FilterEncodingComponent<>(Decoder::filter, Encoder::filter);
    FilterEncodingComponent<Map.Entry<String, String>> ENTRY =
        new FilterEncodingComponent<>(Decoder::entry, Encoder::entry);

    static FilterEncodingComponent<String> string() {
        return STRING;
    }

    static FilterEncodingComponent<Filter> filter() {
        return FILTER;
    }

    static FilterEncodingComponent<Map.Entry<String, String>> entry() {
        return ENTRY;
    }
}
