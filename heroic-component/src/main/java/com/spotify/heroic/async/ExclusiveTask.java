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

package com.spotify.heroic.async;

import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class ExclusiveTask implements Runnable {
    private final Object lock = new Object();
    private volatile AsyncFuture<Void> progress = null;

    private final String name;

    @Override
    public void run() {
        synchronized (lock) {
            if (progress != null) {
                /* silently do not invoke */
                return;
            }

            final AsyncFuture<Void> future;

            try {
                future = invoke();
            } catch (final Exception e) {
                error(e);
                return;
            }

            this.progress = future;
        }
    }

    public void stop() {
        synchronized (lock) {
            if (progress != null) {
                progress.cancel();
                progress = null;
            }
        }
    }

    public void error(final Throwable cause) {
        log.error(name + ": Failed to run task", cause);
    }

    /**
     * Task to run, implemented by user.
     *
     * @throws Exception if unable to run.
     */
    public abstract AsyncFuture<Void> invoke() throws Exception;
}
