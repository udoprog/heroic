package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Status;
import lombok.Data;

@Data
public class ProcessingException extends RuntimeException {
    private final Status status;

    public ProcessingException(final Status status) {
        this(status, status.getReasonPhrase());
    }

    public ProcessingException(final Status status, final String message) {
        super(message);
        this.status = status;
    }

    public ProcessingException(final Status status, final String message, final Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    @Override
    public String toString() {
        return "ProcessingException(status=" + status + ", message=" + getMessage() + ")";
    }
}
