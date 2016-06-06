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

package com.spotify.heroic.metric.disk;

import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

import javax.inject.Inject;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@DiskScope
public class SeriesManager implements LifeCycles {
    public static final String LOCK = "_lock";

    private final AsyncFramework async;
    private final Path root;

    private final Object lock = new Object();

    private volatile FileChannel lockFile;
    private volatile FileLock lockFileLock;

    @Inject
    public SeriesManager(AsyncFramework async, Path root) {
        this.async = async;
        this.root = root;
    }

    @Override
    public void register(final LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    public AsyncFuture<Void> write() {
        return async.resolved();
    }

    private FileChannel commitLog() {
        return null;
    }

    private AsyncFuture<Void> stop() {
        return async.call(() -> {
            synchronized (lock) {
                if (lockFileLock != null) {
                    lockFileLock.release();
                }
            }

            return null;
        });
    }

    private AsyncFuture<Void> start() {
        return async.call(() -> {
            synchronized (lock) {
                lockFile = FileChannel.open(root.resolve(LOCK), StandardOpenOption.CREATE);

                if ((lockFileLock = lockFile.tryLock()) == null) {
                    throw new RuntimeException("Failed to lock: " + root);
                }
            }

            return null;
        });
    }
}
