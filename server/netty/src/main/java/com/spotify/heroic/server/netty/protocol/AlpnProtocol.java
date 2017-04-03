package com.spotify.heroic.server.netty.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.netty.Http1Handler;
import com.spotify.heroic.server.netty.Http2Handler;
import com.spotify.heroic.server.netty.Protocol;
import com.spotify.heroic.server.netty.Tls;
import com.spotify.heroic.server.netty.tls.SelfSignedTls;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2Codec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Perform protocol negotiation over TLS and Application-Layer Protocol Negotiation (ALPN).
 *
 * This is the default way of figuring out which protocol a server speaks over the same port, but
 * requires TLS, which can be inconvenient.
 */
@Slf4j
@Data
@JsonTypeName("alpn")
public class AlpnProtocol implements Protocol {
    private final Tls tls;

    public AlpnProtocol(@JsonProperty("tls") Optional<Tls> tls) {
        this.tls = tls.orElseGet(SelfSignedTls::new);
    }

    @Override
    public ChannelInitializer<SocketChannel> setup(final ServerInstance serverInstance) {
        final SslContext sslContext = tls.setup(true);
        return new AlpnServerChannelInitializer(serverInstance, sslContext);
    }

    @RequiredArgsConstructor
    public class AlpnServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final Http1Handler http1;
        private final Http2Handler http2;
        private final SslContext sslContext;

        AlpnServerChannelInitializer(
            final ServerInstance serverInstance, final SslContext sslContext
        ) {
            this.http1 = new Http1Handler(serverInstance, Optional.empty());
            this.http2 = new Http2Handler(serverInstance);
            this.sslContext = sslContext;
        }

        @Override
        protected void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();

            pipeline.addLast(sslContext.newHandler(ch.alloc()));
            pipeline.addLast(new Http2OrHttpHandler(http1, http2));
        }
    }

    public class Http2OrHttpHandler extends ApplicationProtocolNegotiationHandler {
        private final Http1Handler http1;
        private final Http2Handler http2;

        protected Http2OrHttpHandler(final Http1Handler http1, final Http2Handler http2) {
            super(ApplicationProtocolNames.HTTP_1_1);
            this.http1 = http1;
            this.http2 = http2;
        }

        @Override
        protected void configurePipeline(ChannelHandlerContext ctx, String protocol)
            throws Exception {
            if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                ctx.pipeline().addLast(new Http2Codec(true, http2));
                return;
            }

            if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
                ctx.pipeline().addLast(http1);
                return;
            }

            throw new IllegalStateException("unknown protocol: " + protocol);
        }
    }
}
