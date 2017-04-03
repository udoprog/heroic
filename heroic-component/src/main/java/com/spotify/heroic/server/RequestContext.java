package com.spotify.heroic.server;

import java.util.Optional;

public interface RequestContext {
    Optional<String> getHeader(String name);

    String getRemoteAddress();

    String getRemoteHost();
}
