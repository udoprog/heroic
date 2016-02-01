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

import com.google.protobuf.ByteString;
import com.spotify.heroic.async.AsyncObservable;

import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface DataClient {
    AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, Mutations mutations);

    MutationsBuilder mutations();

    ReadModifyWriteRulesBuilder readModifyWriteRules();

    AsyncFuture<List<Row>> readRows(String tableName, ByteString rowKey, RowFilter filter);

    AsyncFuture<Row> readModifyWriteRow(String tableName, ByteString rowKey,
            ReadModifyWriteRules rules);

    AsyncObservable<Row> rows(RowRange rowRange, RowFilter rowFilter);
}
