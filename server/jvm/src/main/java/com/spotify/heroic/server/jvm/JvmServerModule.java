package com.spotify.heroic.server.jvm;

import static java.util.Optional.of;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.server.ServerModule;
import com.spotify.heroic.server.ServerSetup;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class JvmServerModule implements ServerModule {
    private final JvmServerEnvironment environment;

    @Override
    public ServerSetup module(final PrimaryComponent primary) {
        return new JvmServerSetup(primary.async(), environment.setup(primary.async()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ServerModule.Builder {
        private Optional<JvmServerEnvironment> environment = Optional.empty();

        public Builder environment(final JvmServerEnvironment environment) {
            this.environment = of(environment);
            return this;
        }

        public JvmServerModule build() {
            final JvmServerEnvironment environment =
                this.environment.orElseGet(JvmServerEnvironment::new);
            return new JvmServerModule(environment);
        }
    }
}
