package com.spotify.heroic.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Optional;

public class EmptyHeaders implements Headers {
    @Override
    public Optional<String> first(final String name) {
        return Optional.empty();
    }

    @Override
    public List<String> get(final String name) {
        return ImmutableList.of();
    }

    @Override
    public Multimap<String, String> all() {
        return ImmutableMultimap.of();
    }
}
