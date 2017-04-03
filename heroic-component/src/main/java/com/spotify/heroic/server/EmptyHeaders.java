package com.spotify.heroic.server;

import java.util.Optional;
import java.util.stream.Stream;

public class EmptyHeaders implements Headers {
    @Override
    public Optional<CharSequence> first(final CharSequence name) {
        return Optional.empty();
    }

    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        return Stream.empty();
    }

    @Override
    public Stream<Header> all() {
        return Stream.empty();
    }

    @Override
    public Headers join(final Headers headers) {
        return headers;
    }
}
