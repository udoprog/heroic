package com.spotify.heroic.server.netty;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.server.ServerInstance;
import com.spotify.heroic.server.netty.protocol.AlpnProtocol;
import com.spotify.heroic.server.netty.protocol.H2cProtocol;
import com.spotify.heroic.server.netty.protocol.HttpProtocol;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(H2cProtocol.class), @JsonSubTypes.Type(HttpProtocol.class),
    @JsonSubTypes.Type(AlpnProtocol.class)
})
public interface Protocol {
    ChannelInitializer<SocketChannel> setup(final ServerInstance server);
}
