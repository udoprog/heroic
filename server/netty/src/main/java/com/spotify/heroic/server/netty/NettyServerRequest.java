package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.ServerRequest;
import lombok.Data;

@Data
public class NettyServerRequest implements ServerRequest {
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
        return new NettyServerRequest(method, path, this.headers.join(headers));
    }
}

