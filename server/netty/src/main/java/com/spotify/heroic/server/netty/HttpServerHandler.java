package com.spotify.heroic.server.netty;

import com.spotify.heroic.server.BodyWriter;
import com.spotify.heroic.server.OnceObserver;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.ServerRequest;
import com.spotify.heroic.server.ServerResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public final class HttpServerHandler extends MessageToMessageDecoder<HttpObject> {
    private final ServerInstance serverInstance;

    private HttpVersion version = null;
    private BodyWriter pending = null;

    @Override
    protected void decode(
        final ChannelHandlerContext ctx, final HttpObject httpObject, final List<Object> out
    ) throws Exception {
        // no pending internal request, this must be the first
        if (pending == null) {
            if (!(httpObject instanceof HttpRequest)) {
                throw new IllegalStateException("Channel in invalid state");
            }

            final HttpRequest httpRequest = (HttpRequest) httpObject;

            version = httpRequest.protocolVersion();

            final NettyHttpHeaders nettyHttpHeaders = new NettyHttpHeaders(httpRequest.headers());

            final ServerRequest serverRequest =
                new NettyServerRequest(httpRequest.method().name(), httpRequest.uri(),
                    nettyHttpHeaders);

            final Channel ch = ctx.channel();
            pending = serverInstance.call(serverRequest, new SendResponseObserver(ch, version));
            return;
        }

        assert version != null;
        assert pending != null;

        if (httpObject instanceof HttpContent) {
            final HttpContent httpContent = (HttpContent) httpObject;

            pending.write(httpContent.content().nioBuffer());

            if (httpContent instanceof LastHttpContent) {
                pending.end();
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        throws Exception {
        log.error("error in channel", cause);
        ctx.close();
    }

    @Slf4j
    @RequiredArgsConstructor
    static class SendResponseObserver implements OnceObserver<ServerResponse> {
        private final Channel ch;
        private final HttpVersion version;

        @Override
        public void observe(final ServerResponse result) {
            final ByteBuf buffer =
                result.entity().map(Unpooled::wrappedBuffer).orElse(Unpooled.EMPTY_BUFFER);

            final FullHttpResponse response = new DefaultFullHttpResponse(version,
                HttpResponseStatus.valueOf(result.status().getStatusCode()), buffer);

            response.headers().set("content-length", buffer.readableBytes());

            result
                .headers()
                .all()
                .forEach(pair -> response.headers().set(pair.getName(), pair.getValue()));

            ch.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
                ch.close();

                if (!future.isSuccess()) {
                    SendResponseObserver.log.error("Failed to flush response", future.cause());
                }
            });
        }

        @Override
        public void fail(final Throwable error) {
            SendResponseObserver.log.error("Failed to receive server response", error);
            ch.close();
        }
    }
}
