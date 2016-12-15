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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A collection of metrics.
 *
 * These are guaranteed to be stored in any particular order.
 *
 * <p>These collections to not generate additional internal copies for the data they are being
 * presented with. It is important that the collections being provided are not modified while this
 * is in scope.
 *
 * @author udoprog
 * @see com.spotify.heroic.metric.Point
 * @see com.spotify.heroic.metric.Spread
 * @see com.spotify.heroic.metric.Event
 * @see com.spotify.heroic.metric.MetricGroup
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(SortedCollection.Points.class),
    @JsonSubTypes.Type(SortedCollection.Events.class),
    @JsonSubTypes.Type(SortedCollection.Spreads.class),
    @JsonSubTypes.Type(SortedCollection.Cardinality.class),
    @JsonSubTypes.Type(SortedCollection.Groups.class)
})
public interface SortedCollection extends BaseCollection {
    List<? extends Metric> data();

    @SuppressWarnings("unchecked")
    static SortedCollection build(
        final MetricType type, final List<? extends Metric> metrics
    ) {
        switch (type) {
            case POINT:
                return new Points((List<Point>) metrics);
            case EVENT:
                return new Events((List<Event>) metrics);
            case SPREAD:
                return new Spreads((List<Spread>) metrics);
            case GROUP:
                return new Groups((List<MetricGroup>) metrics);
            case CARDINALITY:
                return new Cardinality((List<Payload>) metrics);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    @JsonTypeName("points")
    @Data
    class Points implements SortedCollection {
        private final List<Point> data;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Point.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePoints(tags, series, data, data.size());
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public MetricType type() {
            return MetricType.POINT;
        }

        @Override
        public List<Point> data() {
            return data;
        }
    }

    @JsonTypeName("events")
    @Data
    class Events implements SortedCollection {
        private final List<Event> data;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Event.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateEvents(tags, series, data, data.size());
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public MetricType type() {
            return MetricType.EVENT;
        }

        @Override
        public List<Event> data() {
            return data;
        }
    }

    @JsonTypeName("spreads")
    @Data
    class Spreads implements SortedCollection {
        private final List<Spread> data;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Spread.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateSpreads(tags, series, data, data.size());
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public MetricType type() {
            return MetricType.SPREAD;
        }

        @Override
        public List<Spread> data() {
            return data;
        }
    }

    @JsonTypeName("groups")
    @Data
    class Groups implements SortedCollection {
        private final List<MetricGroup> data;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != MetricGroup.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateGroup(tags, series, data, data.size());
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public MetricType type() {
            return MetricType.GROUP;
        }

        @Override
        public List<MetricGroup> data() {
            return data;
        }
    }

    @JsonTypeName("cardinality")
    @Data
    class Cardinality implements SortedCollection {
        private final List<Payload> data;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Payload.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePayload(tags, series, data, data.size());
        }

        @Override
        public long size() {
            return data.size();
        }

        @Override
        public MetricType type() {
            return MetricType.CARDINALITY;
        }

        @Override
        public List<Payload> data() {
            return data;
        }
    }
}
