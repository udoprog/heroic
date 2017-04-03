package com.spotify.heroic.server.jvm;

import com.spotify.heroic.server.ServerInstance;
import eu.toolchain.async.AsyncFramework;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;

public class JvmServerEnvironment {
    private final ConcurrentHashMap<InetSocketAddress, JvmServerInstance> instances =
        new ConcurrentHashMap<>();

    public Instance setup(final AsyncFramework async) {
        return new Instance(async);
    }

    public Optional<JvmServerInstance> lookup(final InetSocketAddress address) {
        return Optional.ofNullable(instances.get(address));
    }

    @RequiredArgsConstructor
    public class Instance {
        private final AsyncFramework async;

        public Runnable bind(final InetSocketAddress bind, final ServerInstance serverInstance) {
            final JvmServerInstance newInstance = new JvmServerInstance(async, serverInstance);

            if (instances.putIfAbsent(bind, newInstance) != null) {
                throw new IllegalStateException("Address already bound: " + bind);
            }

            return () -> instances.remove(bind);
        }
    }
}
