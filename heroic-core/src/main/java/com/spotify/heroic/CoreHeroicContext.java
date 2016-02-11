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

package com.spotify.heroic;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;

@Singleton
public class CoreHeroicContext implements HeroicContext {
    private final AsyncFramework async;

    @Inject
    public CoreHeroicContext(final AsyncFramework async) {
        this.async = async;
    }

    private final Object lock = new Object();
    private volatile ResolvableFuture<Void> startedFuture;

    @Override
    public AsyncFuture<Void> startedFuture() {
        synchronized (lock) {
            if (this.startedFuture == null) {
                this.startedFuture = async.future();
            }
        }

        return this.startedFuture;
    }

    public void resolveCoreFuture() {
        if (this.startedFuture != null) {
            this.startedFuture.resolve(null);
        }
    }
}
