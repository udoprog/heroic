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

import com.spotify.heroic.elasticsearch.client.Client;
import com.spotify.heroic.elasticsearch.client.PutTemplate;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;

/**
 * Common connection abstraction between Node and TransportClient.
 */
@Slf4j
@RequiredArgsConstructor
@ToString(of = {"index", "client"})
public class Connection {
    private final AsyncFramework async;
    private final IndexMapping index;
    private final Client client;

    private final String templateName;
    private final BackendType type;

    public AsyncFuture<Void> close() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(async.call((Callable<Void>) () -> {
            client.close();
            return null;
        }));

        return async.collectAndDiscard(futures);
    }

    public AsyncFuture<Void> configure() {
        log.info("[{}] updating template for {}", templateName, index.template());

        final PutTemplate request =
            new PutTemplate(index.template(), type.getSettings(), type.getMappings());

        return client.putTemplate(request);
    }

    public String[] readIndices() throws NoIndexSelectedException {
        return index.readIndices();
    }

    public String[] writeIndices() throws NoIndexSelectedException {
        return index.writeIndices();
    }

    public SearchRequestBuilder search(String type) throws NoIndexSelectedException {
        return index.search(client, type);
    }

    public CountRequestBuilder count(String type) throws NoIndexSelectedException {
        return index.count(client, type);
    }

    public IndexRequestBuilder index(String index, String type) {
        return client.prepareIndex(index, type);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return client.prepareSearchScroll(scrollId);
    }
}
