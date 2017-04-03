package com.spotify.heroic.lib.httpcore;

import java.nio.charset.Charset;
import lombok.Data;

@Data
public final class AcceptEncoding {
    private final Charset charset;

    public static AcceptEncoding parse(final CharSequence input) {
        return new AcceptEncoding(Charset.forName(input.toString()));
    }

    @Override
    public String toString() {
        return charset.displayName().toLowerCase();
    }
}
