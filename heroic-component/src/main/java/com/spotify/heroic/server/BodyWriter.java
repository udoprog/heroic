package com.spotify.heroic.server;

import java.nio.ByteBuffer;

public interface BodyWriter {
    void write(ByteBuffer buffer);

    void end();
}
