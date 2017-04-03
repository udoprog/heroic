package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.MimeType;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Data;

@Data
class Serializers {
    private final Map<MimeType, Function<SerializeContext, ByteBuffer>> serializers;

    public Optional<Function<SerializeContext, ByteBuffer>> match(final MimeType mime) {
        return Optional.ofNullable(serializers.get(mime));
    }
}
