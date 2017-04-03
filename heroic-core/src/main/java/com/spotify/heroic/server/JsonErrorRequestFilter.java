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

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.common.Validation;
import com.spotify.heroic.lib.httpcore.Status;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Data
public class JsonErrorRequestFilter implements RequestFilter {
    private static final BodyWriter IGNORE_BODY = new BodyWriter() {
        @Override
        public void write(final ByteBuffer buffer) {
        }

        @Override
        public void end() {
        }
    };

    private final ObjectMapper mapper;

    @Override
    public BodyWriter filter(
        final ServerRequest originalRequest, final OnceObserver<ServerResponse> response,
        final BiFunction<ServerRequest, OnceObserver<ServerResponse>, BodyWriter> next
    ) {
        final OnceObserver<ServerResponse> nextObserver = new OnceObserver<ServerResponse>() {
            @Override
            public void observe(final ServerResponse result) {
                response.observe(result);
            }

            @Override
            public void fail(final Throwable error) {
                // preempt error and convert to response
                handleException(response, error);
            }
        };

        final BodyWriter writer;

        try {
            writer = next.apply(originalRequest, nextObserver);
        } catch (final Exception e) {
            handleException(response, e);
            return IGNORE_BODY;
        }

        return new BodyWriter() {
            @Override
            public void write(final ByteBuffer buffer) {
                try {
                    writer.write(buffer);
                } catch (final Exception e) {
                    handleException(response, e);
                }
            }

            @Override
            public void end() {
                try {
                    writer.end();
                } catch (final Exception e) {
                    handleException(response, e);
                }
            }
        };
    }

    private void handleException(
        final OnceObserver<ServerResponse> response, final Throwable e
    ) {
        if (e instanceof HandlerException) {
            handleException(response, e.getCause());
            return;
        }

        if (e instanceof JsonParseException) {
            handleJsonParseException(response, (JsonParseException) e);
            return;
        }

        if (e instanceof JsonMappingException) {
            handleJsonMappingException(response, (JsonMappingException) e);
            return;
        }

        if (e instanceof ProcessingException) {
            final Status status = ((ProcessingException) e).getStatus();
            writeResponse(response, new ErrorMessage(e.getMessage(), status), status);
            return;
        }

        writeResponse(response,
            new ErrorMessage(e.getClass().getCanonicalName() + ": " + e.getMessage(),
                Status.INTERNAL_SERVER_ERROR), Status.INTERNAL_SERVER_ERROR);
    }

    public void handleJsonParseException(
        final OnceObserver<ServerResponse> response, final JsonParseException e
    ) {
        final JsonLocation l = e.getLocation();

        writeResponse(response,
            new JsonParseErrorMessage(e.getOriginalMessage(), Status.BAD_REQUEST, l.getLineNr(),
                l.getColumnNr()), Status.BAD_REQUEST);
    }

    public void handleJsonMappingException(
        final OnceObserver<ServerResponse> response, final JsonMappingException e
    ) {
        final String path = constructPath(e);

        writeResponse(response,
            new JsonErrorMessage(e.getOriginalMessage(), Status.BAD_REQUEST, path),
            Status.BAD_REQUEST);
    }

    private String constructPath(final JsonMappingException e) {
        final StringBuilder builder = new StringBuilder();

        final Consumer<String> field = name -> {
            if (builder.length() > 0) {
                builder.append(".");
            }

            builder.append(name);
        };

        for (final JsonMappingException.Reference reference : e.getPath()) {
            if (reference.getIndex() >= 0) {
                builder.append("[").append(reference.getIndex()).append("]");
            } else {
                field.accept(reference.getFieldName());
            }
        }

        if (e.getCause() instanceof Validation.MissingField) {
            final Validation.MissingField f = (Validation.MissingField) e.getCause();
            field.accept(f.getName());
        }

        return builder.toString();
    }

    private void writeResponse(
        final OnceObserver<ServerResponse> response, final Object entityObject, final Status status
    ) {
        final ByteBuffer entity;

        try {
            entity = ByteBuffer.wrap(mapper.writeValueAsBytes(entityObject));
        } catch (JsonProcessingException e2) {
            throw new RuntimeException(e2);
        }

        response.observe(
            new ImmediateServerResponse(status, Headers.of("content-type", "application/json"),
                Optional.of(entity)));
    }

    public static class InternalErrorMessage extends ErrorMessage {
        public InternalErrorMessage(final String message, final Status status) {
            super(message, status);
        }

        public String getType() {
            return "internal-error";
        }
    }

    public static class JsonErrorMessage extends InternalErrorMessage {
        @Getter
        private final String path;

        public JsonErrorMessage(final String message, final Status status, final String path) {
            super(message, status);
            this.path = path;
        }

        @Override
        public String getType() {
            return "json-error";
        }
    }

    public static class JsonParseErrorMessage extends ErrorMessage {
        @Getter
        private final int line;
        @Getter
        private final int col;

        public JsonParseErrorMessage(
            final String message, final Status status, final int line, final int col
        ) {
            super(message, status);
            this.line = line;
            this.col = col;
        }

        public String getType() {
            return "json-parse-error";
        }
    }

    @RequiredArgsConstructor
    public static class ErrorMessage {
        @Getter
        private final String message;
        @Getter
        private final String reason;
        @Getter
        private final int status;

        public ErrorMessage(final String message, final Status status) {
            this.message = message;
            this.reason = status.getReasonPhrase();
            this.status = status.getStatusCode();
        }

        public String getType() {
            return "error";
        }
    }
}
