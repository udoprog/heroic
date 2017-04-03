package com.spotify.heroic.server;

import lombok.Data;

@Data
class SerializeContext {
    private final Response response;
    private final Object entity;
}
