package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.Accept;
import com.spotify.heroic.lib.httpcore.AcceptEncoding;
import com.spotify.heroic.lib.httpcore.ContentType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public interface Headers {
    Pattern VALUE_SEPARATOR = Pattern.compile(",");

    Stream<CharSequence> getRaw(CharSequence name);

    /**
     * Use for inter-operability. Returns a completely unmodified view of all headers.
     *
     * @return all headers as pairs
     */
    Stream<Header> all();

    default Stream<CharSequence> get(CharSequence name) {
        return getRaw(name)
            .flatMap(value -> Stream.of(VALUE_SEPARATOR.split(value)))
            .map(String::trim);
    }

    default Optional<CharSequence> first(CharSequence name) {
        return get(name).findFirst();
    }

    default Headers join(Headers headers) {
        final List<Headers> allHeaders = new ArrayList<>();

        if (this instanceof JoinedHeaders) {
            allHeaders.addAll(((JoinedHeaders) this).getHeaders());
        } else if (!(this instanceof EmptyHeaders)) {
            allHeaders.add(this);
        }

        if (headers instanceof JoinedHeaders) {
            allHeaders.addAll(((JoinedHeaders) headers).getHeaders());
        } else if (!(headers instanceof EmptyHeaders)) {
            allHeaders.add(headers);
        }

        return new JoinedHeaders(allHeaders);
    }

    default Optional<Accept> accept() {
        return first("accept").map(Accept::parse);
    }

    default Optional<AcceptEncoding> acceptEncoding() {
        return first("accept-encoding").map(AcceptEncoding::parse);
    }

    default Optional<ContentType> contentType() {
        return first("content-type").map(ContentType::parse);
    }

    default Optional<Integer> contentLength() {
        return first("content-length").map(s -> Integer.valueOf(s.toString()));
    }

    static Headers empty() {
        return new EmptyHeaders();
    }

    static Headers of(String name, String value) {
        Header.validateName(name);
        return new SingleHeader(name, value);
    }
}
