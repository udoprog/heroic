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

package com.spotify.heroic.http.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RequestError;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

@RequiredArgsConstructor
@JsonSerialize(using = QueryMetricsResponse.Serializer.class)
public class QueryMetricsResponse {
    @Getter
    private final List<AggregationData> data;

    @Getter
    private final Duration cadence;

    @Getter
    private final List<RequestError> errors;

    @Getter
    private final QueryTrace trace;

    public static class Serializer extends JsonSerializer<QueryMetricsResponse> {
        @Override
        public void serialize(
            QueryMetricsResponse response, JsonGenerator g, SerializerProvider provider
        ) throws IOException {
            final List<AggregationData> result = response.getData();
            final Map<String, String> common = calculateCommon(result);

            g.writeStartObject();

            final Optional<DateRange> range = totalRange(response.getData());

            if (range.isPresent()) {
                g.writeObjectField("range", range.get());
            } else {
                g.writeNullField("range");
            }

            g.writeObjectField("trace", response.getTrace());

            g.writeFieldName("common");
            g.writeObject(common);

            g.writeFieldName("result");
            serializeResult(g, common, result);

            g.writeFieldName("errors");
            serializeErrors(g, response.getErrors());

            g.writeNumberField("cadence", response.getCadence().toMilliseconds());

            g.writeEndObject();
        }

        private Optional<DateRange> totalRange(final List<AggregationData> data) {
            Optional<DateRange> range = Optional.empty();

            for (final AggregationData d : data) {
                range = Optional.of(range.map(r -> r.join(d.getRange())).orElseGet(d::getRange));
            }

            return range;
        }

        private void serializeCommonTags(
            final JsonGenerator g, final Map<String, SortedSet<String>> common
        ) throws IOException {
            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> e : common.entrySet()) {
                g.writeFieldName(e.getKey());

                g.writeStartArray();

                for (final String value : e.getValue()) {
                    g.writeString(value);
                }

                g.writeEndArray();
            }

            g.writeEndObject();
        }

        private void serializeErrors(final JsonGenerator g, final List<RequestError> errors)
            throws IOException {
            g.writeStartArray();

            for (final RequestError error : errors) {
                g.writeObject(error);
            }

            g.writeEndArray();
        }

        private Map<String, String> calculateCommon(final List<AggregationData> data) {
            final CommonTags common = new CommonTags();

            for (final AggregationData d : data) {
                for (final Series s : d.getSeries()) {
                    for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                        common.put(e.getKey(), e.getValue());
                    }
                }
            }

            return common.common;
        }

        private void serializeResult(
            final JsonGenerator g, final Map<String, String> common,
            final List<AggregationData> result
        ) throws IOException {
            g.writeStartArray();

            for (final AggregationData d : result) {
                g.writeStartObject();

                final MetricCollection collection = d.getMetrics();

                g.writeObjectField("range", d.getRange());
                g.writeStringField("type", collection.getType().identifier());
                g.writeStringField("hash", Integer.toHexString(d.getKey().hashCode()));
                g.writeObjectField("values", collection.getData());

                writeSeries(g, d.getSeries(), common);

                g.writeEndObject();
            }

            g.writeEndArray();
        }

        void writeSeries(
            JsonGenerator g, final Iterable<Series> series, final Map<String, String> common
        ) throws IOException {
            final Set<String> keys = new HashSet<>();
            final Map<String, Integer> counts = new HashMap<>();
            final Map<String, Set<String>> values = new HashMap<>();
            final CommonTags singles = new CommonTags();

            for (final Series s : series) {
                keys.add(s.getKey());

                for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                    if (!common.containsKey(e.getKey())) {
                        Set<String> v = values.get(e.getKey());

                        if (v == null) {
                            v = new HashSet<>();
                            values.put(e.getKey(), v);
                        }

                        v.add(e.getValue());

                        if (v.size() > 1) {
                            counts.put(e.getKey(), v.size());
                        }

                        singles.put(e.getKey(), e.getValue());
                    }
                }
            }

            g.writeObjectField("keys", keys);
            g.writeObjectField("tagCounts", counts);
            g.writeObjectField("tags", singles.common);
        }
    }

    static class CommonTags {
        final Map<String, String> common = new HashMap<>();
        final Set<String> blacklist = new HashSet<>();

        public void put(final String key, final String value) {
            if (blacklist.contains(key)) {
                return;
            }

            final String previous = common.put(key, value);

            if (previous == null) {
                return;
            }

            if (previous.equals(value)) {
                return;
            }

            blacklist.add(key);
            common.remove(key);
        }
    }
}
