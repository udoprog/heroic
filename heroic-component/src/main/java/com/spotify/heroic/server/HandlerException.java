package com.spotify.heroic.server;

public class HandlerException extends RuntimeException {
    public HandlerException(final Throwable cause) {
        super(cause);
    }
}
