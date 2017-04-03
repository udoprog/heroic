package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.Header;
import com.spotify.heroic.server.Headers;
import io.netty.handler.codec.http2.Http2Headers;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NettyHttp2Headers implements Headers {
    private final Http2Headers headers;

    @Override
    public Optional<CharSequence> first(final CharSequence name) {
        return Optional.ofNullable(headers.get(name));
    }

    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        return headers.getAll(name).stream();
    }

    @Override
    public Stream<Header> all() {
        return StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(headers.iterator(), Spliterator.ORDERED),
                false)
            .map(e -> Header.of(e.getKey(), e.getValue()));
    }
}
