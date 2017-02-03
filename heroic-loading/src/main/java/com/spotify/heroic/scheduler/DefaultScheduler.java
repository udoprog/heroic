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

package com.spotify.heroic.scheduler;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@ToString(exclude = {"scheduler"})
public class DefaultScheduler implements Scheduler {
    private static final String UNKNOWN = "unknown";

    private final AsyncFramework async;
    private final ScheduledExecutorService scheduler;

    @Override
    public void periodically(long value, final TimeUnit unit, final Task task) {
        periodically(UNKNOWN, value, unit, task);
    }

    @Override
    public void periodically(
        final String name, final long value, final TimeUnit unit, final Task task
    ) {
        final Runnable refreshCluster = new Runnable() {
            @Override
            public void run() {
                try {
                    task.run();
                } catch (InterruptedException e) {
                    log.debug("task interrupted");
                } catch (final Exception e) {
                    log.error("task '{}' failed", name, e);
                }

                scheduler.schedule(this, value, unit);
            }
        };

        scheduler.schedule(refreshCluster, value, unit);
    }

    @Override
    public void schedule(long value, TimeUnit unit, final Task task) {
        schedule(UNKNOWN, value, unit, task);
    }

    @Override
    public void schedule(final String name, long value, TimeUnit unit, final Task task) {
        scheduler.schedule(() -> {
            try {
                task.run();
            } catch (final Exception e) {
                log.error("{} task failed", name, e);
            }
        }, value, unit);
    }

    @Override
    public UniqueTaskHandle unique(final String name) {
        return new UniqueTaskHandleImpl(name);
    }

    @Data
    class UniqueTaskHandleImpl implements UniqueTaskHandle {
        private final String name;
        private final Object lock = new Object();

        private volatile UniqueTask current;
        private volatile AsyncFuture<Void> currentFuture;
        private volatile QueuedUniqueTask deferred;
        private volatile boolean stopped = false;

        @Override
        public void schedule(final long value, final TimeUnit unit, final UniqueTask task) {
            synchronized (lock) {
                if (stopped) {
                    return;
                }

                if (current != null) {
                    // to be run immediately after this one
                    deferred = new QueuedUniqueTask(value, unit, task);
                    return;
                }

                current = task;

                if (value <= 0) {
                    this.runScheduled();
                } else {
                    scheduler.schedule(this::runScheduled, value, unit);
                }
            }
        }

        @Override
        public void scheduleNow(final UniqueTask task) {
            schedule(0, TimeUnit.SECONDS, task);
        }

        @Override
        public AsyncFuture<Void> stop() {
            synchronized (lock) {
                if (stopped) {
                    return async.failed(new IllegalStateException("already stopped"));
                }

                this.stopped = true;

                if (currentFuture != null) {
                    return currentFuture;
                }
            }

            return async.resolved();
        }

        private void runScheduled() {
            synchronized (lock) {
                if (stopped) {
                    return;
                }

                log.trace("{}: running scheduled", name);

                final AsyncFuture<Void> future;

                try {
                    future = current.run();
                } catch (final Exception e) {
                    log.error("{} task failed", name, e);
                    finishTask();
                    return;
                }

                currentFuture = future.onFinished(this::finishTask);
            }
        }

        private void finishTask() {
            synchronized (lock) {
                log.trace("{}: finishing", name);

                current = null;
                currentFuture = null;

                if (stopped) {
                    return;
                }

                if (deferred != null) {
                    log.trace("{}: picking up deferred task", name);
                    final QueuedUniqueTask q = this.deferred;
                    this.deferred = null;
                    schedule(q.getValue(), q.getUnit(), q.getTask());
                }
            }
        }
    }

    @Data
    public static class QueuedUniqueTask {
        private final long value;
        private final TimeUnit unit;
        private final UniqueTask task;
    }
}
