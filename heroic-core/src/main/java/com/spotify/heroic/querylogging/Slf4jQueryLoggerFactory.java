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

package com.spotify.heroic.querylogging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

@QueryLoggingScope
public class Slf4jQueryLoggerFactory implements QueryLoggerFactory {
    private final String loggerName;
    private final ObjectMapper objectMapper;

    @Inject
    public Slf4jQueryLoggerFactory(
        @Named("queryLoggerSlf4jName") String loggerName,
        @Named(HeroicCore.APPLICATION_JSON) ObjectMapper objectMapper
    ) {
        this.loggerName = loggerName;
        this.objectMapper = objectMapper;
    }

    @Override
    public Slf4jQueryLogger create(String component) {
        final Logger logger = LoggerFactory.getLogger(loggerName);
        return new Slf4jQueryLogger(logger, objectMapper, component);
    }
}
