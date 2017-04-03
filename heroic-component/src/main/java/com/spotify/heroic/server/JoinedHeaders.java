package com.spotify.heroic.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Data;

@Data
public class JoinedHeaders implements Headers {
    private final List<Headers> headers;

    @Override
    public Optional<CharSequence> first(final CharSequence name) {
        for (final Headers h : headers) {
            final Optional<CharSequence> v = h.first(name);

            if (v.isPresent()) {
                return v;
            }
        }

        return Optional.empty();
    }

    @Override
    public Stream<CharSequence> getRaw(final CharSequence name) {
        return headers.stream().map(h -> h.getRaw(name)).reduce(Stream.empty(), Stream::concat);
    }

    @Override
    public Stream<Header> all() {
        return headers.stream().map(Headers::all).reduce(Stream.empty(), Stream::concat);
    }

    @Override
    public Headers join(final Headers headers) {
        final List<Headers> newHeaders = new ArrayList<>(this.headers);

        if (headers instanceof JoinedHeaders) {
            newHeaders.addAll(((JoinedHeaders) headers).headers);
            return new JoinedHeaders(newHeaders);
        } else {
            newHeaders.add(headers);
        }

        return new JoinedHeaders(newHeaders);
    }
}
