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

package com.spotify.heroic.http.render;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.querylogging.QueryContext;
import eu.toolchain.async.AsyncFuture;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.Optional;
import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jfree.chart.JFreeChart;

@Path("render")
public class RenderResource {
    private static final int DEFAULT_WIDTH = 600;
    private static final int DEFAULT_HEIGHT = 400;

    private final ObjectMapper mapper;
    private final QueryManager query;

    @Inject
    public RenderResource(
        @Named(MediaType.APPLICATION_JSON) ObjectMapper mapper, QueryManager query
    ) {
        this.mapper = mapper;
        this.query = query;
    }

    @SuppressWarnings("unchecked")
    @GET
    @Path("image")
    @Produces("image/png")
    public AsyncFuture<Response> render(
        @QueryParam("q") String queryString, @QueryParam("backend") Optional<String> backendGroup,
        @QueryParam("title") String title, @QueryParam("width") Optional<Integer> width,
        @QueryParam("height") final Optional<Integer> height,
        @QueryParam("threshold") Optional<Double> threshold
    ) {
        final int w = width.orElse(DEFAULT_WIDTH);
        final int h = width.orElse(DEFAULT_HEIGHT);

        final QueryContext queryContext = QueryContext.empty();
        final Query q = query.newQueryFromString(queryString).build();

        return this.query
            .useOptionalGroup(backendGroup)
            .query(q, queryContext)
            .directTransform(result -> {
                final JFreeChart chart =
                    RenderUtils.createChart(result.getGroups(), title, threshold, h);

                final BufferedImage image = chart.createBufferedImage(w, h);

                final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ImageIO.write(image, "png", buffer);

                return Response.ok(buffer.toByteArray()).build();
            });
    }
}
