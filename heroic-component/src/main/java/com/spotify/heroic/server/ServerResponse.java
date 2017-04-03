package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Status;
import java.nio.ByteBuffer;
import java.util.Optional;

public interface ServerResponse {
    /**
     * The status code of the response.
     */
    Status status();

    /**
     * The headers of the response.
     */
    Headers headers();

    /**
     * The reported size of the response.
     */
    Optional<ByteBuffer> entity();

    /**
     * Return a response with a modified set of headers.
     */
    ServerResponse withHeaders(Headers headers);

    static ServerResponse notFound() {
        return new ImmediateServerResponse(Status.NOT_FOUND, Headers.empty(), Optional.empty());
    }

    static ServerResponse expectationFailed() {
        return new ImmediateServerResponse(Status.EXPECTATION_FAILED, Headers.empty(),
            Optional.empty());
    }
}
