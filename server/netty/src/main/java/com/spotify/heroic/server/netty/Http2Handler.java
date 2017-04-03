package com.spotify.heroic.server.netty;

import com.spotify.heroic.lib.httpcore.Status;
import com.spotify.heroic.server.BodyWriter;
import com.spotify.heroic.server.OnceObserver;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerRequest;
import com.spotify.heroic.server.ServerResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@ChannelHandler.Sharable
@RequiredArgsConstructor
@Slf4j
public final class Http2Handler extends ChannelHandlerAdapter {
    private final ServerInstance serverInstance;

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        ctx.pipeline().addLast("http2-per-stream", new PerStreamHandler());
    }

    private static Http2Headers http1HeadersToHttp2Headers(final FullHttpRequest request) {
        final HttpHeaders source = request.headers();

        final Http2Headers headers = new DefaultHttp2Headers()
            .authority(source.get("host"))
            .method(request.method().name())
            .path(request.uri())
            .scheme("http");

        for (final Map.Entry<String, String> e : source.entries()) {
            headers.set(e.getKey().toLowerCase(), e.getValue());
        }

        return headers;
    }

    class PerStreamHandler extends ChannelDuplexHandler {
        private BodyWriter pending = null;

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
            throws Exception {
            if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
                HttpServerUpgradeHandler.UpgradeEvent upgradeEvent =
                    (HttpServerUpgradeHandler.UpgradeEvent) evt;

                Http2Headers headers = http1HeadersToHttp2Headers(upgradeEvent.upgradeRequest());
                onHeadersRead(ctx, headers, false);
                onDataRead(ctx, upgradeEvent.upgradeRequest().content(), true);
            }

            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg)
            throws Exception {
            if (msg instanceof Http2HeadersFrame) {
                final Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
                onHeadersRead(ctx, headersFrame.headers(), headersFrame.isEndStream());
            } else if (msg instanceof Http2DataFrame) {
                final Http2DataFrame dataFrame = (Http2DataFrame) msg;
                onDataRead(ctx, dataFrame.content(), dataFrame.isEndStream());
            } else {
                super.channelRead(ctx, msg);
            }
        }

        public int onDataRead(
            final ChannelHandlerContext ctx, final ByteBuf buffer, final boolean endStream
        ) throws Http2Exception {
            pending.write(buffer.nioBuffer());

            if (endStream) {
                pending.end();
            }

            return 0;
        }

        public void onHeadersRead(
            final ChannelHandlerContext ctx, final Http2Headers headers, final boolean endStream
        ) throws Http2Exception {
            final NettyHttp2Headers h = new NettyHttp2Headers(headers);

            final String method = headers.method().toString();
            final String uri = headers.path().toString();

            final ServerRequest serverRequest = new NettyServerRequest(method, uri, h);

            pending = serverInstance.call(serverRequest, new SendResponseObserver(ctx));

            if (endStream) {
                pending.end();
            }
        }

        @RequiredArgsConstructor
        class SendResponseObserver implements OnceObserver<ServerResponse> {
            private final ChannelHandlerContext ctx;

            @Override
            public void observe(final ServerResponse result) {
                final ByteBuf buffer =
                    result.entity().map(Unpooled::wrappedBuffer).orElse(Unpooled.EMPTY_BUFFER);

                final Http2Headers headers =
                    new DefaultHttp2Headers().status(Integer.toString(Status.OK.getStatusCode()));
                result
                    .headers()
                    .all()
                    .forEach(pair -> headers.set(pair.getName(), pair.getValue()));
                headers.set("content-length", Integer.toString(buffer.readableBytes()));

                ctx.write(new DefaultHttp2HeadersFrame(headers));
                ctx.write(new DefaultHttp2DataFrame(buffer, true));
                ctx.flush();
            }

            @Override
            public void fail(final Throwable error) {
                log.error("Failed to receive server response", error);
                ctx.close();
            }
        }
    }
}
