package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.ServerHandle;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerSetup;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.Optional;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyServerSetup implements ServerSetup {
    private final AsyncFramework async;
    private final Protocol protocol;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final int parentThreads;
    private final int childThreads;

    @Inject
    public NettyServerSetup(final AsyncFramework async, final NettyServerConfig module) {
        this.async = async;

        this.protocol = module.getProtocol();
        this.host = module.getHost();
        this.port = module.getPort();
        this.parentThreads = module.getParentThreads();
        this.childThreads = module.getChildThreads();
    }

    @Override
    public AsyncFuture<ServerHandle> bind(
        final String defaultHost, final Integer defaultPort, final ServerInstance serverInstance
    ) {
        final String host = this.host.orElse(defaultHost);
        final int port = this.port.orElse(defaultPort);

        final ResolvableFuture<ServerHandle> future = async.future();

        final EventLoopGroup parentGroup = new NioEventLoopGroup(parentThreads);
        final EventLoopGroup childGroup = new NioEventLoopGroup(childThreads);

        final ServerBootstrap b = new ServerBootstrap();

        final ChannelInitializer<SocketChannel> initializer = protocol.setup(serverInstance);

        b
            .group(parentGroup, childGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(initializer);

        b.bind(host, port).addListener((ChannelFutureListener) channelFuture -> {
            if (!channelFuture.isSuccess()) {
                future.fail(channelFuture.cause());
                return;
            }

            final Channel channel = channelFuture.channel();

            future.resolve(() -> {
                return async.call(() -> {
                    channel.close().sync();
                    parentGroup.shutdownGracefully();
                    childGroup.shutdownGracefully();
                    return null;
                });
            });
        });

        return future;
    }

    @RequiredArgsConstructor
    public static class HttpServerChannelInitializer extends ChannelInitializer<NioSocketChannel> {
        private final ServerInstance serverInstance;

        @Override
        protected void initChannel(final NioSocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpServerHandler(serverInstance));
        }
    }
}
