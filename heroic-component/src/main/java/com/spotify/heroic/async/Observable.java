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

import com.spotify.heroic.common.Throwing;
import eu.toolchain.async.Transform;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public interface Observable<T> {
    void observe(Observer<T> observer) throws Exception;

    /**
     * Transform this observable to another type.
     *
     * @param transform The transformation to perform.
     * @return A new Observable of the transformed type.
     */
    default <S> Observable<S> transform(final Transform<T, S> transform) {
        return observer -> {
            observe(new Observer<T>() {
                @Override
                public void observe(T value) throws Exception {
                    observer.observe(transform.transform(value));
                }

                @Override
                public void fail(Throwable cause) throws Exception {
                    observer.fail(cause);
                }

                @Override
                public void end() throws Exception {
                    observer.end();
                }
            });
        };
    }

    default Observable<T> onFinished(final ObservableFinished end) {
        return observer -> {
            observe(new Observer<T>() {
                @Override
                public void observe(T value) throws Exception {
                    observer.observe(value);
                }

                @Override
                public void fail(Throwable cause) throws Exception {
                    Throwing.call(() -> observer.fail(cause), end::finished);
                }

                @Override
                public void end() throws Exception {
                    Throwing.call(observer::end, end::finished);
                }
            });
        };
    }

    static <T> Observable<T> chain(final List<Observable<T>> observables) {
        return observer -> {
            final Iterator<Observable<T>> it = observables.iterator();

            if (!it.hasNext()) {
                observer.end();
                return;
            }

            final Observer<T> chainer = new Observer<T>() {
                final AtomicBoolean stackGuard = new AtomicBoolean();

                @Override
                public void observe(T value) throws Exception {
                    observer.observe(value);
                }

                @Override
                public void fail(Throwable cause) throws Exception {
                    observer.fail(cause);
                }

                @Override
                public void end() throws Exception {
                    /* avoid blowing up the stack if call is immediate */
                    while (true) {
                        if (!stackGuard.compareAndSet(false, true)) {
                            break;
                        }

                        if (!it.hasNext()) {
                            observer.end();
                            break;
                        }

                        try {
                            it.next().observe(this);
                        } catch (final Exception e) {
                            observer.fail(e);
                            break;
                        }

                        /* attempt to set it back */
                        if (!stackGuard.compareAndSet(true, false)) {
                            break;
                        }
                    }
                }
            };

            it.next().observe(chainer);
        };
    }

    Queue<?> EMPTY_QUEUE = new ArrayDeque<>();

    static <T> Observable<T> chainConcurrent(
        final List<Observable<T>> observables, final int concurrency
    ) {
        return new Observable<T>() {
            @Override
            public void observe(final Observer<T> observer) throws Exception {
                if (observables.isEmpty()) {
                    observer.end();
                    return;
                }

                final int concurrent = Math.min(concurrency, observables.size());
                final AtomicInteger running = new AtomicInteger(concurrent);

                final Queue<Observable<T>> queue;
                final List<Observable<T>> initial;

                if (concurrent >= observables.size()) {
                    queue = (Queue<Observable<T>>) EMPTY_QUEUE;
                    initial = observables;
                } else {
                    queue = new ConcurrentLinkedQueue<>(
                        observables.subList(concurrent, observables.size()));
                    initial = observables.subList(0, concurrent);
                }

                final Observer<T> chainer = new Observer<T>() {
                    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                    volatile boolean error = false;

                    @Override
                    public void observe(T value) throws Exception {
                        observer.observe(value);
                    }

                    @Override
                    public void fail(Throwable cause) throws Exception {
                        error = true;
                        errors.add(cause);
                        checkEnd();
                    }

                    @Override
                    public void end() throws Exception {
                        final Observable<T> next = queue.poll();

                        if (error || next == null) {
                            checkEnd();
                            return;
                        }

                        next.observe(this);
                    }

                    void checkEnd() throws Exception {
                        if (running.decrementAndGet() == 0) {
                            doEnd();
                        }
                    }

                    void doEnd() throws Exception {
                        if (errors.size() > 0) {
                            final Throwable first = errors.poll();

                            while (true) {
                                final Throwable next = errors.poll();

                                if (next == null) {
                                    break;
                                }

                                first.addSuppressed(next);
                            }

                            observer.fail(first);
                            return;
                        }

                        observer.end();
                    }
                };

                for (final Observable<T> o : initial) {
                    o.observe(chainer);
                }
            }
        };
    }

    static <T> Observable<T> concurrently(final List<Observable<T>> observables) {
        return (final Observer<T> observer) -> {
            if (observables.isEmpty()) {
                observer.end();
                return;
            }

            final AtomicInteger running = new AtomicInteger(observables.size());

            final Observer<T> obs = new Observer<T>() {
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                volatile boolean error = false;

                @Override
                public void observe(T value) throws Exception {
                    observer.observe(value);
                }

                @Override
                public void fail(Throwable cause) throws Exception {
                    error = true;
                    errors.add(cause);
                    end();
                }

                @Override
                public void end() throws Exception {
                    if (running.decrementAndGet() != 0) {
                        return;
                    }

                    doEnd();
                }

                void doEnd() throws Exception {
                    if (errors.size() > 0) {
                        final Throwable first = errors.poll();

                        while (true) {
                            final Throwable next = errors.poll();

                            if (next == null) {
                                break;
                            }

                            first.addSuppressed(next);
                        }

                        observer.fail(first);
                        return;
                    }

                    observer.end();
                }
            };

            for (final Observable<T> o : observables) {
                o.observe(obs);
            }
        };
    }

    static <T> Observable<T> empty() {
        return new Observable<T>() {
            @Override
            public void observe(final Observer<T> observer) throws Exception {
                observer.end();
            }
        };
    }

    /**
     * Create an observable that will always be immediately failed with the given throwable.
     */
    static <T> Observable<T> failed(final Throwable e) {
        return observer -> observer.fail(e);
    }

    static <T> Observable<T> ofValues(final T... values) {
        return observer -> {
            for (final T value : values) {
                observer.observe(value);
            }

            observer.end();
        };
    }
}
