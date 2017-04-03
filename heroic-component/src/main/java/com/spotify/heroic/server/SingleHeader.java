package com.spotify.heroic.server;

import java.util.Optional;
import java.util.stream.Stream;
import lombok.Data;

@Data
public class SingleHeader implements Headers {
    private final String name;
    private final String value;

    @Override
    public Optional<CharSequence> first(final CharSequence name) {
        if (Header.nameEquals(name, name)) {
            return Optional.of(value);
        }

        return Optional.empty();
    }

    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        if (Header.nameEquals(name, name)) {
            return Stream.of(value);
        }

        return Stream.empty();
    }

    @Override
    public Stream<Header> all() {
        return Stream.of(Header.of(name, value));
    }
}
