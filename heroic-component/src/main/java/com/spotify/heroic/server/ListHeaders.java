package com.spotify.heroic.server;

import java.util.List;
import java.util.stream.Stream;
import lombok.Data;

@Data
public class ListHeaders implements Headers {
    private final List<Header> headers;

    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        return headers
            .stream()
            .filter(p -> Header.nameEquals(name, p.getName()))
            .map(Header::getValue);
    }

    @Override
    public Stream<Header> all() {
        return headers.stream();
    }
}
