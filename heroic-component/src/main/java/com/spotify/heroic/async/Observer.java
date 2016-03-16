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
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureResolved;

/**
 * XXX: consider replacing with RxJava at some point.
 *
 * @param <T>
 * @author udoprog
 */
public interface Observer<T> {
    void observe(final T value) throws Exception;

    void fail(Throwable cause) throws Exception;

    void end() throws Exception;

    default Observer<T> onFinished(ObservableFinished finished) {
        return new Observer<T>() {
            @Override
            public void observe(T value) throws Exception {
                Observer.this.observe(value);
            }

            @Override
            public void fail(Throwable cause) throws Exception {
                Throwing.call(() -> Observer.this.fail(cause), finished::finished);
            }

            @Override
            public void end() throws Exception {
                Throwing.call(Observer.this::end, finished::finished);
            }
        };
    }

    default <R> FutureDone<R> bindResolved(
        final FutureResolved<R> next
    ) {
        return new FutureDone<R>() {
            @Override
            public void failed(final Throwable cause) throws Exception {
                fail(cause);
            }

            @Override
            public void cancelled() throws Exception {
                end();
            }

            @Override
            public void resolved(final R result) throws Exception {
                next.resolved(result);
            }
        };
    }

    default FutureDone<Void> bindVoid() {
        return bindResolved(v -> {
        });
    }
}
