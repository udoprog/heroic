package com.spotify.heroic.server;

import com.spotify.heroic.lib.httpcore.ContentType;
import java.util.List;
import java.util.Optional;
import lombok.Data;

@Data
class RouteEntry {
    private final Optional<RouteTarget> defaultMapping;
    private final List<ServerHandlerBuilder.MimeTypeRoute> routes;

    public Optional<RouteTarget> resolve(final Optional<ContentType> contentType) {
        return contentType.map(this::matchRoute).orElse(defaultMapping);
    }

    private Optional<RouteTarget> matchRoute(final ContentType contentType) {
        for (final ServerHandlerBuilder.MimeTypeRoute route : routes) {
            if (route.getMimeType().matches(contentType.getMimeType())) {
                return Optional.of(route.getTarget());
            }
        }

        return Optional.empty();
    }
}
