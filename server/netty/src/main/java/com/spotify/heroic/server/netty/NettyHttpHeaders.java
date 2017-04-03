package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.Header;
import com.spotify.heroic.server.Headers;
import io.netty.handler.codec.http.HttpHeaders;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class NettyHttpHeaders implements Headers {
    private final HttpHeaders headers;

    public NettyHttpHeaders(final HttpHeaders headers) {
        this.headers = headers;
    }

    @Override
    public Optional<CharSequence> first(final CharSequence name) {
        return Optional.ofNullable(headers.get(name));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        return (Stream<CharSequence>) (Stream<? extends CharSequence>) headers
            .getAll(name)
            .stream();
    }

    @Override
    public Stream<Header> all() {
        return StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(headers.iteratorAsString(),
                Spliterator.ORDERED), false)
            .map(e -> Header.of(e.getKey(), e.getValue()));
    }
}
