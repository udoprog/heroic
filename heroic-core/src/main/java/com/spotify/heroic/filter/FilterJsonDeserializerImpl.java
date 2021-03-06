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
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import com.spotify.heroic.filter.Filter.Raw;
import com.spotify.heroic.grammar.QueryParser;

public class FilterJsonDeserializerImpl extends JsonDeserializer<Filter>
        implements FilterJsonDeserializer {
    private final Map<String, FilterJsonSerialization<? extends Filter>> impl = new HashMap<>();

    @Inject
    private QueryParser parser;

    @Override
    public <T extends Filter> void register(String id, FilterJsonSerialization<T> serializer) {
        impl.put(id, serializer);
    }

    public void configure(SimpleModule module) {
        module.addDeserializer(Filter.class, this);
    }

    @Override
    public Filter deserialize(JsonParser p, DeserializationContext c)
            throws IOException, JsonProcessingException {
        if (p.getCurrentToken() != JsonToken.START_ARRAY) {
            throw c.mappingException("Expected start of array");
        }

        if (p.nextToken() != JsonToken.VALUE_STRING) {
            throw c.mappingException("Expected operator (string)");
        }

        final String operator = p.readValueAs(String.class);

        final FilterJsonSerialization<? extends Filter> deserializer = impl.get(operator);

        if (deserializer == null) {
            throw c.mappingException("No such operator: " + operator);
        }

        p.nextToken();

        final FilterJsonSerialization.Deserializer d = new Deserializer(p, c);

        final Filter filter = deserializer.deserialize(d);

        if (p.getCurrentToken() != JsonToken.END_ARRAY) {
            throw c.mappingException("Expected end of array from '" + deserializer + "'");
        }

        if (filter instanceof Filter.Raw) {
            return parseRawFilter((Filter.Raw) filter);
        }

        return filter.optimize();
    }

    private Filter parseRawFilter(Raw filter) {
        return parser.parseFilter(filter.first());
    }

    @RequiredArgsConstructor
    private static final class Deserializer implements FilterJsonSerialization.Deserializer {
        private final JsonParser parser;
        private final DeserializationContext c;

        @Override
        public String string() throws IOException {
            if (parser.getCurrentToken() == JsonToken.END_ARRAY) {
                return null;
            }

            if (parser.getCurrentToken() != JsonToken.VALUE_STRING) {
                throw c.mappingException("Expected string");
            }

            final String string = parser.getValueAsString();

            parser.nextToken();
            return string;
        }

        @Override
        public Filter filter() throws IOException {
            if (parser.getCurrentToken() == JsonToken.END_ARRAY) {
                return null;
            }

            if (parser.getCurrentToken() != JsonToken.START_ARRAY) {
                throw c.mappingException("Expected start of new filter expression");
            }

            final Filter filter = parser.readValueAs(Filter.class);

            parser.nextToken();
            return filter;
        }
    }
}
