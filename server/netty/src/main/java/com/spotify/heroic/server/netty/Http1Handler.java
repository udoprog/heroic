package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.ServerInstance;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@ChannelHandler.Sharable
@RequiredArgsConstructor
public class Http1Handler extends ChannelHandlerAdapter {
    private final ServerInstance serverInstance;
    private final Optional<SslContext> sslContext;

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        final ChannelPipeline pipeline = ctx.pipeline();

        sslContext.ifPresent(sslContext -> {
            pipeline.addLast(sslContext.newHandler(ctx.alloc()));
        });

        pipeline.addLast("http-codec", new HttpServerCodec());
        pipeline.addLast("http-handler", new HttpServerHandler(serverInstance));
    }
}
