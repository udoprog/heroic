package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.MimeType;
import java.util.function.Function;
import lombok.Data;

@Data
class MimeTypeDeserializer {
    private final MimeType mimeType;
    private final Function<DeserializeContext, Object> deserializer;
}
