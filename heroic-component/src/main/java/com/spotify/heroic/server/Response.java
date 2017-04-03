package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Status;
import java.util.Optional;
import lombok.Data;

@Data
public class Response {
    private final Status status;
    private final Optional<Object> entity;
    private final Headers headers;

    public static Response status(final Status status) {
        return new Response(status, Optional.empty(), Headers.empty());
    }

    public static Response status(final Status status, final Object entity) {
        return new Response(status, Optional.of(entity), Headers.empty());
    }

    public static Response ok(final Object entity) {
        return new Response(Status.OK, Optional.of(entity), Headers.empty());
    }
}
