/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.querylogging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.Query;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryTrace;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.UUID;

@QueryLoggingScope
@Slf4j
@RequiredArgsConstructor
public class Slf4jQueryLogger implements QueryLogger {
    private final Logger queryLog;
    private final ObjectMapper objectMapper;
    private final String component;

    @Override
    public void logHttpQueryText(
        final QueryContext context, final String query, final HttpContext httpContext
    ) {
        try {
            final JsonNode queryJsonNode = objectMapper.valueToTree(query);
            final HttpQueryData data = new HttpQueryData(httpContext, queryJsonNode);
            serializeAndLog(context, "http-query-text", data);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logHttpQueryJson(
        final QueryContext context, final QueryMetrics query, final HttpContext httpContext
    ) {
        try {
            final JsonNode queryJsonNode = objectMapper.valueToTree(query);
            final HttpQueryData data = new HttpQueryData(httpContext, queryJsonNode);
            serializeAndLog(context, "http-query-json", data);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logQuery(final QueryContext context, final Query query) {
        serializeAndLog(context, "query", query);
    }

    @Override
    public void logOutgoingRequestToShards(
        final QueryContext context, final FullQuery.Request request
    ) {
        try {
            final FullQuery.Request.Summary summary = request.summarize();
            serializeAndLog(context, "outgoing-request-to-shards", summary);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logIncomingRequestAtNode(
        final QueryContext context, final FullQuery.Request request
    ) {
        try {
            final FullQuery.Request.Summary summary = request.summarize();
            serializeAndLog(context, "incoming-request-at-node", summary);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logOutgoingResponseAtNode(final QueryContext context, final FullQuery response) {
        try {
            final FullQuery.Summary summary = response.summarize();
            serializeAndLog(context, "outgoing-response-at-node", summary);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logIncomingResponseFromShard(
        final QueryContext context, final FullQuery response
    ) {
        try {
            final FullQuery.Summary summary = response.summarize();
            serializeAndLog(context, "incoming-response-from-shard", summary);
        } catch (Exception e) {
            handleError(e);
        }
    }

    @Override
    public void logQueryTrace(final QueryContext context, final QueryTrace queryTrace) {
        serializeAndLog(context, "query-trace", queryTrace);
    }

    @Override
    public void logFinalResponse(
        final QueryContext context, final QueryMetricsResponse queryMetricsResponse
    ) {
        try {
            final QueryMetricsResponse.Summary summary = queryMetricsResponse.summarize();
            serializeAndLog(context, "final-response", summary);
        } catch (Exception e) {
            handleError(e);
        }
    }

    private <T> void serializeAndLog(
        final QueryContext context, final String type, final T data
    ) {
        try {
            final MessageFormat<T> message =
                new MessageFormat<>(component, context.getQueryId(), context.getClientContext(),
                    type, data);

            final String timestamp = Instant.now().toString();
            final LogFormat logFormat = new LogFormat(timestamp, message);
            queryLog.trace(objectMapper.writeValueAsString(logFormat));
        } catch (Exception e) {
            handleError(e);
        }
    }

    void handleError(final Exception e) {
        log.error("Failed while trying to log query", e);
    }

    @Data
    public class LogFormat {
        @JsonProperty("@timestamp")
        private final String timestamp;
        @JsonProperty("@message")
        private final MessageFormat message;
    }

    @Data
    public class MessageFormat<T> {
        private final String component;
        private final UUID queryId;
        private final JsonNode clientContext;
        private final String type;
        private final T data;
    }

    @Data
    public class HttpQueryData {
        private final HttpContext httpContext;
        private final JsonNode query;
    }
}
