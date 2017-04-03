package com.spotify.heroic.server.netty.protocol;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.netty.Http1Handler;
import com.spotify.heroic.server.netty.Protocol;
import com.spotify.heroic.server.netty.Tls;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Only support HTTP/1.1 (over TLS).
 */
@Slf4j
@Data
@JsonTypeName("http")
public class HttpProtocol implements Protocol {
    private final Optional<Tls> tls;

    @Override
    public ChannelInitializer<SocketChannel> setup(final ServerInstance serverInstance) {
        final Http1Handler http1 =
            new Http1Handler(serverInstance, tls.map(tls -> tls.setup(false)));
        return new HttpServerChannelInitializer(http1);
    }

    @RequiredArgsConstructor
    public class HttpServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final Http1Handler http1;

        @Override
        protected void initChannel(final SocketChannel ch) throws Exception {
            ch.pipeline().addLast(http1);
        }
    }
}
