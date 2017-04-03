package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.ContentType;
import com.spotify.heroic.lib.httpcore.MimeType;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.Data;

@Data
class Deserializers {
    private final List<MimeTypeDeserializer> deserializers;

    public Optional<Function<DeserializeContext, Object>> match(final ContentType contentType) {
        final MimeType mime = contentType.getMimeType();

        for (final MimeTypeDeserializer deserializer : deserializers) {
            if (mime.matches(deserializer.getMimeType())) {
                return Optional.of(deserializer.getDeserializer());
            }
        }

        return Optional.empty();
    }
}
