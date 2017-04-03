package com.spotify.heroic.server;

import eu.toolchain.rs.RsMapping;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import lombok.Data;

@Data
class DeserializeContext {
    private final String method;
    private final String path;
    private final Headers headers;
    private final RsMapping<?> mapping;
    private final ByteBuffer entity;
    private final Type type;
}
