package com.spotify.heroic.server;

import java.nio.ByteBuffer;
import javax.ws.rs.core.Response;

public interface ServerResponse {
    ByteBuffer EMPTY_BODY = ByteBuffer.allocate(0);

    /**
     * The status code of the response.
     */
    Response.Status status();

    /**
     * The headers of the response.
     */
    Headers headers();

    /**
     * Consume the body of the response asynchronously.
     */
    void body(Observer<ByteBuffer> observer);

    static ServerResponse notFound() {
        return new CompletedServerResponse(Response.Status.NOT_FOUND, Headers.empty(), EMPTY_BODY);
    }

    static ServerResponse ok(ByteBuffer body) {
        return new CompletedServerResponse(Response.Status.OK, Headers.empty(), body);
    }
}
