package com.spotify.heroic.lib.httpcore;

import com.google.common.collect.ImmutableList;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Data;

@Data
public final class ContentType {
    public static Pattern ELEMENT_SEPARATOR = Pattern.compile(";");

    public static final ContentType APPLICATION_JSON = create(MimeType.APPLICATION_JSON);
    public static final ContentType TEXT_PLAIN = create(MimeType.TEXT_PLAIN);
    public static final ContentType TEXT_XML = create(MimeType.TEXT_XML);
    public static final ContentType WILDCARD = create(MimeType.WILDCARD);

    private final MimeType mimeType;
    private final Optional<Charset> charset;
    private final List<NameValuePair> params;

    public static ContentType create(final MimeType mimeType) {
        return new ContentType(mimeType, Optional.of(StandardCharsets.UTF_8), ImmutableList.of());
    }

    public static ContentType create(final MimeType mimeType, final Charset charset) {
        return new ContentType(mimeType, Optional.of(StandardCharsets.UTF_8), ImmutableList.of());
    }

    public static ContentType parse(final CharSequence input) {
        final String[] parts = ELEMENT_SEPARATOR.split(input);

        if (parts.length == 0) {
            throw new IllegalArgumentException(input.toString());
        }

        final MimeType mimeType = MimeType.parse(parts[0]);
        Optional<Charset> charset = Optional.empty();
        final List<NameValuePair> params = new ArrayList<>();

        for (int i = 1; i < parts.length; i++) {
            final NameValuePair pair = NameValuePair.parse(parts[i]);

            if ("charset".equals(pair.getName())) {
                charset = Optional.of(Charset.forName(pair.getValue()));
                continue;
            }

            params.add(pair);
        }

        return new ContentType(mimeType, charset, params);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append(mimeType.toString());

        charset.ifPresent(c -> {
            builder.append("; charset=").append(c.name().toLowerCase());
        });

        params.forEach(param -> {
            builder.append("; ").append(param.getName()).append("=").append(param.getValue());
        });

        return builder.toString();
    }
}
