package com.spotify.heroic.server.netty;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.server.netty.tls.CertificateTls;
import com.spotify.heroic.server.netty.tls.SelfSignedTls;
import io.netty.handler.ssl.SslContext;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(CertificateTls.class), @JsonSubTypes.Type(SelfSignedTls.class)})
public interface Tls {
    /**
     * Setup the SslContext according to the given configuration.
     *
     * @param alpnRequired is ALPN required or not
     * @return a new SslContext
     */
    SslContext setup(boolean alpnRequired);
}
