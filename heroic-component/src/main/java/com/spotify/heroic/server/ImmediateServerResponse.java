package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Status;
import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/**
 * A server response that has been completed early.
 */
@RequiredArgsConstructor
public class ImmediateServerResponse implements ServerResponse {
    private final Status status;
    private final Headers headers;
    private final Optional<ByteBuffer> entity;

    @Override
    public Status status() {
        return status;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public Optional<ByteBuffer> entity() {
        return entity;
    }

    @Override
    public ServerResponse withHeaders(final Headers headers) {
        return new ImmediateServerResponse(status, headers, entity);
    }
}
