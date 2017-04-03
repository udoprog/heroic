package com.spotify.heroic.server.netty.tls;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.netty.Tls;
import io.netty.handler.ssl.SslContext;

@JsonTypeName("certificate")
public class CertificateTls implements Tls {
    @Override
    public SslContext setup(final boolean alpnRequired) {
        throw new RuntimeException("not supported");
    }
}
