package com.spotify.heroic.server.netty.tls;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.netty.Tls;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.security.cert.CertificateException;
import java.util.List;
import javax.net.ssl.SSLException;

@JsonTypeName("self-signed")
public class SelfSignedTls implements Tls {
    @Override
    public SslContext setup(final boolean alpnRequired) {
        final SslProvider provider =
            alpnRequired && OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;

        final SelfSignedCertificate ssc;

        try {
            ssc = new SelfSignedCertificate();
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }

        /*
         * A good default list of ciphers to support, regardless of aiming for HTTP/2 or not.
         */
        final List<String> ciphers = Http2SecurityUtil.CIPHERS;

        final SslContextBuilder builder = SslContextBuilder
            .forServer(ssc.certificate(), ssc.privateKey())
            .sslProvider(provider)
            .ciphers(ciphers, SupportedCipherSuiteFilter.INSTANCE);

        if (alpnRequired) {
            builder.applicationProtocolConfig(
                new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1));
        }

        try {
            return builder.build();
        } catch (final SSLException e) {
            throw new RuntimeException(e);
        }
    }
}
