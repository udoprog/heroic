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

package com.spotify.heroic.cache.memory;

import com.spotify.heroic.QueryInstance;
import com.spotify.heroic.cache.CacheScope;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFuture;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@CacheScope
public class MemoryQueryCache implements QueryCache {
    private final ExpiringMap<QueryInstance, AsyncFuture<QueryResult>> cache;

    private final Object lock = new Object();

    @Inject
    public MemoryQueryCache() {
        this.cache = ExpiringMap.builder().variableExpiration().build();
    }

    @Override
    public AsyncFuture<QueryResult> load(
        QueryInstance query, Supplier<AsyncFuture<QueryResult>> loader
    ) {
        final Optional<Long> cadence =
            query.getAggregation().cadence().map(Duration::toMilliseconds).filter(c -> c > 0);

        /* can't be cached :( */
        if (cadence.isPresent()) {
            return loader.get();
        }

        final AsyncFuture<QueryResult> result = cache.get(query);

        if (result != null) {
            return result;
        }

        synchronized (lock) {
            final AsyncFuture<QueryResult> candidate = cache.get(query);

            if (candidate != null) {
                return candidate;
            }

            final AsyncFuture<QueryResult> next = loader.get();
            cache.put(query, next, ExpirationPolicy.ACCESSED, cadence.get(), TimeUnit.MILLISECONDS);
            return next;
        }
    }
}
