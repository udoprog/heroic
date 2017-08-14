package com.spotify.heroic.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.heroic.proto.heroic.QueryMetrics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

@RequiredArgsConstructor
public class HttpClient implements Client {
    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    private final OkHttpClient client;
    private final ObjectMapper mapper;
    private final String url;

    public CompletableFuture<QueryMetrics.Response> queryMetrics(final QueryMetrics QueryMetrics) {
        return request("query/metrics", b -> b.post(jsonRequestBody(QueryMetrics)))
            .thenApply(handleError())
            .thenApply(deserializeResponse(QueryMetrics.Response.class));
    }

    private Function<Response, Response> handleError() {
        return response -> {
            if (!response.isSuccessful()) {
                try {
                    throw new RuntimeException(
                        "request failed: " + response.code() + ": " + response.body().string());
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }

            return response;
        };
    }

    private <T> RequestBody jsonRequestBody(final T value) {
        final byte[] body;

        try {
            body = mapper.writeValueAsBytes(value);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException("failed to serialize body", e);
        }

        System.out.println(new String(body, StandardCharsets.UTF_8));
        return RequestBody.create(APPLICATION_JSON, body);
    }

    private <T> Function<Response, T> deserializeResponse(final Class<T> expected) {
        return response -> {
            final ResponseBody body = response.body();

            if (body == null) {
                throw new RuntimeException("no body");
            }

            try {
                return mapper.readValue(body.byteStream(), expected);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private CompletableFuture<Response> request(
        final String path, final Function<Request.Builder, Request.Builder> fn
    ) {
        final Request request = fn
            .apply(new Request.Builder()
                .url(url + "/" + path)
                .header("content-type", APPLICATION_JSON.toString()))
            .build();

        final Call call = client.newCall(request);

        final CompletableFuture<Response> future = new CompletableFuture<>();

        call.enqueue(new Callback() {
            @Override
            public void onFailure(final Call call, final IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(final Call call, final Response response) throws IOException {
                future.complete(response);
            }
        });

        return future;
    }

    public static Builder builder(final String url) {
        return new Builder(url);
    }

    @RequiredArgsConstructor
    public static class Builder {
        private final String url;

        public HttpClient build() {
            final OkHttpClient client = new OkHttpClient();
            final ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new Jdk8Module());
            return new HttpClient(client, mapper, url);
        }
    }
}
