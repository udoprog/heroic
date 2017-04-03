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

package com.spotify.heroic.http;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.http.cluster.ClusterResource_Binding;
import com.spotify.heroic.http.metadata.MetadataResource_Binding;
import com.spotify.heroic.http.parser.ParserResource_Binding;
import com.spotify.heroic.http.query.QueryResource_Binding;
import com.spotify.heroic.http.render.RenderResource_Binding;
import com.spotify.heroic.http.status.StatusResource_Binding;
import com.spotify.heroic.http.write.WriteResource_Binding;

public class Module implements HeroicModule {
    @Override
    public Runnable setup(final LoadingComponent loading) {
        final HeroicConfigurationContext config = loading.heroicConfigurationContext();

        return () -> {
            config.resources(core -> {
                final HttpResourcesComponent w =
                    DaggerHttpResourcesComponent.builder().coreComponent(core).build();

                return configurator -> {
                    configurator.addRoutes(new HeroicResource_Binding(w.heroicResource()));
                    configurator.addRoutes(new WriteResource_Binding(w.writeResource()));
                    configurator.addRoutes(new StatusResource_Binding(w.statusResource()));
                    configurator.addRoutes(new RenderResource_Binding(w.renderResource()));
                    configurator.addRoutes(new QueryResource_Binding(w.queryResource()));
                    configurator.addRoutes(new MetadataResource_Binding(w.metadataResource()));
                    configurator.addRoutes(new ClusterResource_Binding(w.clusterResource()));
                    configurator.addRoutes(new ParserResource_Binding(w.parserResource()));
                };
            });
        };
    }
}
