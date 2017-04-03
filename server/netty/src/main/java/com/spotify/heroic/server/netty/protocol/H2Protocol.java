package com.spotify.heroic.server.netty.protocol;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.netty.ExceptionHandler;
import com.spotify.heroic.server.netty.Http2Handler;
import com.spotify.heroic.server.netty.Protocol;
import com.spotify.heroic.server.netty.Tls;
import com.spotify.heroic.server.netty.UserEventLogger;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2Codec;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Only support HTTP/2 (over TLS).
 *
 * This require prior knowledge that HTTP/2 is in use, but doesn't require the ALPN extension.
 */
@Slf4j
@Data
@JsonTypeName("h2")
public class H2Protocol implements Protocol {
    private final Optional<Tls> tls;

    @Override
    public ChannelInitializer<SocketChannel> setup(final ServerInstance serverInstance) {
        return new H2cServerChannelInitializer(serverInstance, tls.map(tls -> tls.setup(false)));
    }

    @RequiredArgsConstructor
    public class H2cServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final Http2Handler http2;
        private final Optional<SslContext> sslContext;
        private final UserEventLogger userEventLogger;
        private final ChannelHandler exceptionHandler;

        public H2cServerChannelInitializer(
            final ServerInstance serverInstance, final Optional<SslContext> sslContext
        ) {
            this.http2 = new Http2Handler(serverInstance);
            this.sslContext = sslContext;

            this.userEventLogger = new UserEventLogger();
            this.exceptionHandler = new ExceptionHandler();
        }

        @Override
        protected void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();

            sslContext.ifPresent(sslContext -> {
                pipeline.addLast(sslContext.newHandler(ch.alloc()));
            });

            pipeline.addLast("http2-codec", new Http2Codec(true, http2));
            pipeline.addLast("user-event-logger", userEventLogger);
            pipeline.addLast("exception-handler", exceptionHandler);
        }
    }
}
