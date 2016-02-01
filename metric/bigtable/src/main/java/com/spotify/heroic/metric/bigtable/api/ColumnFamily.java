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

package com.spotify.heroic.metric.bigtable.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Data;

@Data
public class ColumnFamily {
    final String clusterUri;
    final String tableId;
    final String name;

    private static final Pattern NAME_PATTERN = Pattern.compile(
            "^(.+)\\/tables\\/([_a-zA-Z0-9][-_.a-zA-Z0-9]*)\\/columnFamilies\\/([_a-zA-Z0-9][-_.a-zA-Z0-9]*)$");

    private static final String NAME_FORMAT = "%s/tables/%s/columnFamilies/%s";

    public static ColumnFamily fromPb(final com.google.bigtable.admin.table.v1.ColumnFamily value) {
        final Matcher m = NAME_PATTERN.matcher(value.getName());

        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "Not a valid column family name: " + value.getName());
        }

        final String clusterId = m.group(1);
        final String tableId = m.group(2);
        final String name = m.group(3);

        return new ColumnFamily(clusterId, tableId, name);
    }

    public String toURI() {
        return String.format(NAME_FORMAT, clusterUri, tableId, name);
    }
}
