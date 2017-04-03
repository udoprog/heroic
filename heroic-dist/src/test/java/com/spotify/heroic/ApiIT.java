package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.server.Headers;
import com.spotify.heroic.server.ServerResponse;
import com.spotify.heroic.server.jvm.JvmServerEnvironment;
import com.spotify.heroic.server.jvm.JvmServerInstance;
import com.spotify.heroic.server.jvm.JvmServerModule;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.junit.Test;

public class ApiIT extends AbstractSingleNodeIT {
    public static ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private JvmServerEnvironment environment;

    @Override
    protected void beforeCore() {
        setupService = true;
    }

    @Override
    protected HeroicConfig.Builder setupConfig() {
        environment = new JvmServerEnvironment();

        return HeroicConfig
            .builder()
            .host("localhost")
            .port(1234)
            .servers(
                ImmutableList.of(new JvmServerModule.Builder().environment(environment).build()));
    }

    @Test
    public void testSomething() throws Exception {
        final JvmServerInstance instance = environment
            .lookup(InetSocketAddress.createUnresolved("localhost", 1234))
            .orElseThrow(() -> new IllegalArgumentException("no such server"));

        final ServerResponse response =
            instance.call("GET", "status", Headers.empty(), EMPTY).get();

        System.out.println(response.status());
    }
}
