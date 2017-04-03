package com.spotify.heroic.server.netty.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.netty.ExceptionHandler;
import com.spotify.heroic.server.netty.Http2Handler;
import com.spotify.heroic.server.netty.HttpServerHandler;
import com.spotify.heroic.server.netty.Protocol;
import com.spotify.heroic.server.netty.UserEventLogger;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2Codec;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.util.AsciiString;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Assume HTTP/1.1, but support a cleartext upgrade to HTTP/2 through the Upgrade header and
 * negotiation.
 */
@Slf4j
@Data
@JsonTypeName("h2c")
public class H2cProtocol implements Protocol {
    public static final int DEFAULT_MAX_UPGRADE_SIZE = 1 << 16;

    private final int maxUpgradeSize;

    public H2cProtocol(
        @JsonProperty("maxUpgradeSize") final Optional<Integer> maxUpgradeSize
    ) {
        this.maxUpgradeSize = maxUpgradeSize.orElse(DEFAULT_MAX_UPGRADE_SIZE);
    }

    @Override
    public ChannelInitializer<SocketChannel> setup(final ServerInstance serverInstance) {
        return new H2cServerChannelInitializer(serverInstance);
    }

    @RequiredArgsConstructor
    public class H2cServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ServerInstance serverInstance;
        private final Http2Handler http2;
        private final UserEventLogger userEventLogger;
        private final ChannelHandler exceptionHandler;
        private HttpServerUpgradeHandler.UpgradeCodecFactory upgradeCodecFactory;

        public H2cServerChannelInitializer(final ServerInstance serverInstance) {
            this.serverInstance = serverInstance;
            this.http2 = new Http2Handler(serverInstance);

            this.userEventLogger = new UserEventLogger();
            this.exceptionHandler = new ExceptionHandler();

            upgradeCodecFactory = protocol -> {
                if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME,
                    protocol)) {
                    return new Http2ServerUpgradeCodec(new Http2Codec(true, http2));
                } else {
                    return null;
                }
            };
        }

        @Override
        protected void initChannel(final SocketChannel ch) throws Exception {
            final HttpServerCodec httpServerCodec = new HttpServerCodec();

            final ChannelPipeline pipeline = ch.pipeline();

            pipeline.addLast(httpServerCodec);
            pipeline.addLast(
                new HttpServerUpgradeHandler(httpServerCodec, upgradeCodecFactory, maxUpgradeSize));
            pipeline.addLast("http-handler", new HttpServerHandler(serverInstance));
            pipeline.addLast("user-event-logger", userEventLogger);
            pipeline.addLast("exception-handler", exceptionHandler);
        }
    }
}
