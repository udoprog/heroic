package com.spotify.heroic;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.http.status.StatusResponse;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.server.Header;
import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.ListHeaders;
import com.spotify.heroic.server.ServerRequest;
import com.spotify.heroic.server.jvm.JvmServerEnvironment;
import com.spotify.heroic.server.jvm.JvmServerInstance;
import com.spotify.heroic.server.jvm.JvmServerModule;
import eu.toolchain.async.AsyncFuture;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ApiIT extends AbstractSingleNodeIT {
    private final ObjectMapper mapper = HeroicMappers.json(Mockito.mock(QueryParser.class));

    private JvmServerEnvironment environment;
    private JvmServerInstance instance;

    @Override
    protected void beforeCore() {
        setupService = true;
    }

    @Override
    protected HeroicConfig.Builder setupConfig() {
        environment = new JvmServerEnvironment();

        return HeroicConfig
            .builder()
            .host("localhost")
            .port(1234)
            .servers(ImmutableList.of(new JvmServerModule.Builder().environment(environment)));
    }

    @Before
    public void setup() {
        instance = environment
            .lookup(InetSocketAddress.createUnresolved("localhost", 1234))
            .orElseThrow(() -> new IllegalArgumentException("no such server"));
    }

    @Test
    public void testStatus() throws Exception {
        final ImmediateServerRequest request = ImmediateServerRequest
            .builder("GET", "/status")
            .headers(Header.of("accept", "*/*"))
            .build();

        request(request, StatusResponse.class).get();
    }

    @Test
    public void testMetadataSeriesCount() throws Exception {
        final ImmediateServerRequest request = ImmediateServerRequest
            .builder("POST", "/metadata/series-count")
            .headers(Header.of("accept", "*/*"), Header.of("content-type", "application/json"))
            .build();

        final ByteBuffer buffer =
            ByteBuffer.wrap("{\"filter\":[\"key\",\"foo\"]}".getBytes(StandardCharsets.UTF_8));

        final CountSeries countSeries =
            request(request, CountSeries.class, Optional.of(buffer)).get();

        assertEquals(0, countSeries.getCount());
    }

    @Test(expected = Exception.class)
    public void testBadRequest() throws Exception {
        final ImmediateServerRequest request = ImmediateServerRequest
            .builder("POST", "/metadata/series-count")
            .headers(Header.of("accept", "*/*"), Header.of("content-type", "application/json"))
            .build();

        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{Byte.MIN_VALUE});
        request(request, CountSeries.class, Optional.of(buffer)).get();
    }

    private <T> AsyncFuture<T> request(final ServerRequest request, final Class<T> type) {
        return request(request, type, Optional.empty());
    }

    private <T> AsyncFuture<T> request(
        final ServerRequest request, final Class<T> type, final Optional<ByteBuffer> body
    ) {
        return instance.call(request, body).directTransform(response -> {
            return response
                .entity()
                .map(buffer -> deserialize(buffer, type))
                .orElseThrow(() -> new IllegalStateException("no body"));
        });
    }

    private <T> T deserialize(final ByteBuffer out, final Class<T> type) {
        final byte[] array = new byte[out.remaining()];
        out.get(array);

        try {
            return mapper.readValue(array, type);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @RequiredArgsConstructor
    @ToString
    private static class ImmediateServerRequest implements ServerRequest {
        private final String method;
        private final String path;
        private final Headers headers;

        @Override
        public String method() {
            return method;
        }

        @Override
        public String path() {
            return path;
        }

        @Override
        public Headers headers() {
            return headers;
        }

        @Override
        public ServerRequest withHeaders(final Headers headers) {
            return new ImmediateServerRequest(method, path, headers);
        }

        public static Builder builder(final String method, final String path) {
            return new Builder(method, path);
        }

        @RequiredArgsConstructor
        public static class Builder {
            private final String method;
            private final String path;
            private final List<Header> headers = new ArrayList<>();

            public Builder headers(final Header... nameValues) {
                for (final Header nameValue : nameValues) {
                    this.headers.add(nameValue);
                }

                return this;
            }

            public ImmediateServerRequest build() {
                return new ImmediateServerRequest(method, path, new ListHeaders(headers));
            }
        }
    }
}
