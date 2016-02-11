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

package com.spotify.heroic.shell.task;

import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import lombok.Getter;
import lombok.ToString;

@TaskUsage("Delete metadata matching the given query")
@TaskName("metadata-delete")
public class MetadataDelete implements ShellTask {
    private final AsyncFramework async;
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final FilterFactory filters;

    @Inject
    public MetadataDelete(AsyncFramework async, MetadataManager metadata, QueryParser parser,
            FilterFactory filters) {
        this.async = async;
        this.metadata = metadata;
        this.parser = parser;
        this.filters = filters;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        final MetadataBackend group = metadata.useGroup(params.group);

        return group.countSeries(filter).lazyTransform(new LazyTransform<CountSeries, Void>() {
            @Override
            public AsyncFuture<Void> transform(CountSeries c) throws Exception {
                io.out().println(String.format("Deleteing %d entrie(s)", c.getCount()));

                if (!params.ok) {
                    io.out().println("Deletion stopped, use --ok to proceed");
                    return async.resolved(null);
                }

                return group.deleteSeries(filter).directTransform(r -> null);
            }
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
                metaVar = "<group>")
        private String group;

        @Option(name = "--ok", usage = "Verify that you actually want to run")
        private boolean ok = false;

        @Option(name = "--limit", usage = "Limit the number of deletes (default: alot)")
        @Getter
        private int limit = Integer.MAX_VALUE;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }
}
