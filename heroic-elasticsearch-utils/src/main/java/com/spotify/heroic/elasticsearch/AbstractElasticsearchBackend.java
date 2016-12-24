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

package com.spotify.heroic.elasticsearch;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.OptionalLimit;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.ResolvableFuture;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class AbstractElasticsearchBackend {
    public static final TimeValue SCROLL_TIME = TimeValue.timeValueSeconds(5);

    protected final AsyncFramework async;

    protected <T> AsyncFuture<LimitedSet<T>> scrollEntries(
        final Connection c, final SearchRequestBuilder request, final OptionalLimit limit,
        final Function<SearchHit, T> converter
    ) {
        return bind(request.execute()).lazyTransform((initial) -> {
            if (initial.getScrollId() == null) {
                return async.resolved(LimitedSet.of());
            }

            final String scrollId = initial.getScrollId();

            final Supplier<AsyncFuture<SearchResponse>> scroller =
                () -> bind(c.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).execute());

            return scroller
                .get()
                .lazyTransform(new ScrollTransform<>(async, limit, scroller, converter));
        });
    }

    protected <T extends ActionResponse> AsyncFuture<T> bind(
        final ListenableActionFuture<T> actionFuture
    ) {
        final ResolvableFuture<T> future = async.future();

        actionFuture.addListener(new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable e) {
                future.fail(e);
            }
        });

        return future;
    }

    @RequiredArgsConstructor
    public static class ScrollTransform<T> implements LazyTransform<SearchResponse, LimitedSet<T>> {
        private final AsyncFramework async;
        private final OptionalLimit limit;
        private final Supplier<AsyncFuture<SearchResponse>> scroller;

        int size = 0;
        int duplicates = 0;
        final Set<T> results = new HashSet<>();
        final Function<SearchHit, T> converter;

        @Override
        public AsyncFuture<LimitedSet<T>> transform(final SearchResponse response)
            throws Exception {
            final SearchHit[] hits = response.getHits().getHits();

            for (final SearchHit hit : hits) {
                final T convertedHit = converter.apply(hit);

                if (!results.add(convertedHit)) {
                    duplicates += 1;
                } else {
                    size += 1;
                }

                if (limit.isGreater(size)) {
                    results.remove(convertedHit);
                    return async.resolved(new LimitedSet<>(limit.limitSet(results), true));
                }
            }

            if (hits.length == 0) {
                return async.resolved(new LimitedSet<>(results, false));
            }

            return scroller.get().lazyTransform(this);
        }
    }

    @Data
    public static class LimitedSet<T> {
        private final Set<T> set;
        private final boolean limited;

        public static <T> LimitedSet<T> of() {
            return new LimitedSet<>(ImmutableSet.of(), false);
        }
    }
}
