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

package com.spotify.heroic.server;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Data;

@Data
public class CorsRequestFilter implements RequestFilter {
    private final String corsAllowOrigin;

    @Override
    public BodyWriter filter(
        final ServerRequest originalRequest, final OnceObserver<ServerResponse> response,
        final BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> next
    ) {
        final Headers headers = originalRequest.headers();
        final Headers addedHeaders;

        if (originalRequest.method().equalsIgnoreCase("OPTIONS")) {
            final Optional<CharSequence> request = headers.first("access-control-request-headers");
            final Optional<CharSequence> method = headers.first("access-control-request-method");

            if (!(request.isPresent() && method.isPresent())) {
                return next.apply(originalRequest, response);
            }

            addedHeaders = new ListHeaders(
                ImmutableList.of(Header.of("access-control-allow-origin", corsAllowOrigin),
                    Header.of("access-control-allow-methods", method.get()),
                    Header.of("access-control-allow-headers", request.get())));
        } else {
            addedHeaders = new ListHeaders(
                ImmutableList.of(Header.of("access-control-allow-origin", corsAllowOrigin)));
        }

        // modify response
        return next.apply(originalRequest, new OnceObserver<ServerResponse>() {
            @Override
            public void observe(final ServerResponse result) {
                response.observe(result.withHeaders(result.headers().join(addedHeaders)));
            }

            @Override
            public void fail(final Throwable error) {
                response.fail(error);
            }
        });
    }
}
