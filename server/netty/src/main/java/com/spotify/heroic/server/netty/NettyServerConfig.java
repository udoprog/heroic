package com.spotify.heroic.server.netty;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.server.ServerModule;
import com.spotify.heroic.server.ServerSetup;
import com.spotify.heroic.server.netty.protocol.HttpProtocol;
import dagger.Provides;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@dagger.Module
@Data
@RequiredArgsConstructor
public class NettyServerConfig implements ServerModule {
    public static final boolean DEFAULT_H2 = false;
    public static final boolean DEFAULT_H2C = true;

    private final Protocol protocol;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final int parentThreads;
    private final int childThreads;

    @Override
    public ServerSetup module(final PrimaryComponent primary) {
        final NettyServerComponent component = DaggerNettyServerComponent
            .builder()
            .primaryComponent(primary)
            .nettyServerConfig(this)
            .build();

        return component.serverSetup();
    }

    @Provides
    public NettyServerConfig config() {
        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ServerModule.Builder {
        private Optional<Protocol> protocol = Optional.empty();
        private Optional<String> host = Optional.empty();
        private Optional<Integer> port = Optional.empty();
        private Optional<Integer> parentThreads = Optional.empty();
        private Optional<Integer> childThreads = Optional.empty();

        /**
         * Protocol to use.
         */
        public Builder protocol(final Protocol protocol) {
            this.protocol = Optional.of(protocol);
            return this;
        }

        /**
         * Host to bind to.
         */
        public Builder host(final String host) {
            this.host = Optional.of(host);
            return this;
        }

        /**
         * Host to bind to.
         */
        public Builder port(final int port) {
            this.port = Optional.of(port);
            return this;
        }

        /**
         * Number of parent threads accepting connections.
         */
        public Builder parentThreads(final int parentThreads) {
            this.parentThreads = Optional.of(parentThreads);
            return this;
        }

        /**
         * Number of parent threads accepting connections.
         */
        public Builder childThreads(final int childThreads) {
            this.childThreads = Optional.of(childThreads);
            return this;
        }

        @Override
        public NettyServerConfig build() {
            //// ALPN (the future)
            //final Protocol protocol = this.protocol.orElseGet(() -> {
            //    return new HttpProtocol(Optional.of(new SelfSignedTls()));
            //});

            //// HTTP/2 prior w/ knowledge
            //final Protocol protocol = this.protocol.orElseGet(() -> {
            //  return new H2Protocol(Optional.of(new SelfSignedTls()));
            //});

            //// h2c (cleartext upgrade)
            //final Protocol protocol = this.protocol.orElseGet(() -> {
            //    return new H2cProtocol(Optional.empty());
            //});

            // HTTP/1.1
            final Protocol protocol = this.protocol.orElseGet(() -> {
                return new HttpProtocol(Optional.empty());
            });

            final int threadCount = Math.max(Runtime.getRuntime().availableProcessors() * 2, 2);

            final int parentThreads = this.parentThreads.orElse(threadCount);
            final int childThreads = this.parentThreads.orElse(threadCount * 4);

            return new NettyServerConfig(protocol, host, port, parentThreads, childThreads);
        }
    }
}
