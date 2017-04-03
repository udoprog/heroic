package com.spotify.heroic.server;

import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Optional;

public interface Headers {
    Optional<String> first(String name);

    Collection<String> get(String name);

    Multimap<String, String> all();

    static Headers empty() {
        return new EmptyHeaders();
    }
}
