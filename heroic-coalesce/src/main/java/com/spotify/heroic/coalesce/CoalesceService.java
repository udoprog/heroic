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

package com.spotify.heroic.coalesce;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;
import javax.inject.Inject;
import com.spotify.heroic.HeroicBootstrap;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.HeroicInternalLifeCycle;
import com.spotify.heroic.HeroicModules;
import com.spotify.heroic.QueryManager;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoalesceService {
    public static void main(String[] argv) {
        final Parameters params = new Parameters();

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            log.error("Argument error", e);
            System.exit(1);
            return;
        }

        if (params.help) {
            parser.printUsage(System.err);
            HeroicModules.printAllUsage(
                    new PrintWriter(new OutputStreamWriter(System.err, Charsets.UTF_8)), "-P");
            System.exit(0);
            return;
        }

        final CountDownLatch latch = new CountDownLatch(1);

        final HeroicBootstrap bootstrap = new HeroicBootstrap() {
            @Inject
            HeroicInternalLifeCycle lifecycle;

            @Override
            public void run() throws Exception {
                lifecycle.registerShutdown("coalesce", latch::countDown);
            }
        };

        /* create a core which does not attempt to use local backends */
        final HeroicCore core = HeroicCore.builder().disableBackends(true).setupService(false)
                .modules(HeroicModules.ALL_MODULES).configPath(params.heroicConfig)
                .bootstrap(bootstrap).build();

        final HeroicCoreInstance instance;

        try {
            instance = core.start();
        } catch (Exception e) {
            log.error("Failed to start heroic core", e);
            System.exit(1);
            return;
        }

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final CoalesceConfig config;

        try {
            config = mapper.readValue(new File(params.config), CoalesceConfig.class);
        } catch (Exception e) {
            log.error("Failed to read configuration: {}", params.config, e);
            System.exit(1);
            return;
        }

        try {
            instance.injectInstance(CoalesceService.class).run(latch, config);
        } catch (Exception e) {
            log.error("Failed to run coalesce service", e);
        }

        try {
            instance.shutdown();
        } catch (Exception e) {
            log.error("Failed to shutdown heroic core", e);
            System.exit(1);
            return;
        }

        System.exit(0);
    }

    private final QueryManager query;

    @Inject
    public CoalesceService(final QueryManager query) {
        this.query = query;
    }

    public void run(CountDownLatch latch, CoalesceConfig config) throws Exception {
        log.info("Waiting for query engine to initialize...");
        query.initialized().get();

        /* do something */
    }

    @ToString
    private static class Parameters {
        @Option(name = "--config", required = true, usage = "Use coalesce configuration path")
        private String config = null;

        @Option(name = "--heroic-config", required = true, usage = "Use heroic configuration path")
        private String heroicConfig = null;

        @Option(name = "-h", aliases = {"--help"}, help = true, usage = "Display help")
        private boolean help = false;
    }
}
