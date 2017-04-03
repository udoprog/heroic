package com.spotify.heroic.server;

import java.nio.ByteBuffer;
import javax.ws.rs.core.Response;
import lombok.RequiredArgsConstructor;

/**
 * A server response that has been completed early.
 */
@RequiredArgsConstructor
public class CompletedServerResponse implements ServerResponse {
    private final Response.Status status;
    private final Headers headers;
    private final ByteBuffer body;

    @Override
    public Response.Status status() {
        return status;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public void body(final Observer<ByteBuffer> observer) {
        observer.observe(body);
        observer.end();
    }
}
