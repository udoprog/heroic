/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.common.ReflectionUtils;
import com.spotify.heroic.dagger.CoreLoadingComponent;
import com.spotify.heroic.dagger.DaggerCoreLoadingComponent;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.dagger.LoadingModule;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.QueryParser;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

@RequiredArgsConstructor
public class FakeModuleLoader {
    private final LoadingComponent loading;
    private final ObjectMapper json;

    public FakeModuleLoader load(final Class<? extends HeroicModule> module) {
        final HeroicModule instance = ReflectionUtils.buildInstance(module);
        instance.setup(loading).run();
        return this;
    }

    public ObjectMapper config() {
        return loading.configMapper();
    }

    public ObjectMapper json() {
        return json;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Class<? extends HeroicModule>> modules = new ArrayList<>();

        public Builder module(final Class<? extends HeroicModule> module) {
            this.modules.add(module);
            return this;
        }

        public FakeModuleLoader build() {
            final FakeHeroicConfiguration config = new FakeHeroicConfiguration();

            final CoreLoadingComponent loadingComponent = DaggerCoreLoadingComponent
                .builder()
                .loadingModule(new LoadingModule(ForkJoinPool.commonPool(), false, config,
                    ExtraParameters.empty()))
                .build();

            modules.forEach(m -> {
                ReflectionUtils.buildInstance(m).setup(loadingComponent).run();
            });

            final QueryParser parser = new FakeQueryParser();
            final ObjectMapper m = HeroicMappers.json(parser);
            m.registerModule(loadingComponent.aggregationRegistry().module());
            return new FakeModuleLoader(loadingComponent, m);
        }
    }

    public static class FakeHeroicConfiguration implements HeroicConfiguration {
        @Override
        public boolean isDisableLocal() {
            return false;
        }

        @Override
        public boolean isOneshot() {
            return false;
        }
    }

    public static class FakeQueryParser implements QueryParser {
        @Override
        public Filter parseFilter(final String filter) {
            throw new RuntimeException("Cannot parse filter: " + filter);
        }

        @Override
        public List<Expression> parse(final String statements) {
            throw new RuntimeException("Cannot parse statements: " + statements);
        }
    }
}
