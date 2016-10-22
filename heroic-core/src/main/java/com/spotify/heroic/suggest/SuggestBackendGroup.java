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

package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.QueryTrace;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.ToString;

import java.util.List;
import java.util.function.Function;

@Data
@ToString(of = {"backends"})
public class SuggestBackendGroup implements SuggestBackend {
    private static final QueryTrace.Identifier WRITE =
        QueryTrace.identifier(SuggestBackendGroup.class, "write");

    private final AsyncFramework async;
    private final SelectedGroup<SuggestBackend> backends;

    @Override
    public AsyncFuture<Void> configure() {
        return async.collectAndDiscard(run(b -> b.configure()));
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final TagValuesSuggest.Request request) {
        return async.collect(run(b -> b.tagValuesSuggest(request)),
            TagValuesSuggest.reduce(request.getLimit(), request.getGroupLimit()));
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
        return async.collect(run(b -> b.tagValueSuggest(request)),
            TagValueSuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
        return async.collect(run(b -> b.tagKeyCount(request)),
            TagKeyCount.reduce(request.getLimit(), request.getExactLimit()));
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
        return async.collect(run(b -> b.tagSuggest(request)),
            TagSuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
        return async.collect(run(b -> b.keySuggest(request)),
            KeySuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<WriteSuggest> write(final WriteSuggest.Request request) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(WRITE);
        return async.collect(run(b -> b.write(request)), WriteSuggest.reduce(w));
    }

    @Override
    public boolean isReady() {
        boolean ready = true;

        for (final SuggestBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }

    @Override
    public Groups groups() {
        return backends.groups();
    }

    @Override
    public boolean isEmpty() {
        return backends.isEmpty();
    }

    @Override
    public Statistics getStatistics() {
        return backends
            .stream()
            .map(SuggestBackend::getStatistics)
            .reduce(Statistics.empty(), Statistics::merge);
    }

    private <T> List<T> run(final Function<SuggestBackend, T> op) {
        return ImmutableList.copyOf(backends.stream().map(op).iterator());
    }
}
