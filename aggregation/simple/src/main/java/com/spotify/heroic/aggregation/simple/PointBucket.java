/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.metric.Point;
import java.util.Optional;

public interface PointBucket extends Bucket {
    /**
     * Return the point that this bucket corresponds to.
     *
     * @return the point corresponding to this bucket or {@code null} if bucket is not a valid point
     */
    Point asPoint();

    /**
     * Return the bucket as a value.
     *
     * @return the value of the bucket, or {@code Double.NaN} if bucket is not valid.
     */
    default double value() {
        return Optional.ofNullable(asPoint()).map(Point::getValue).orElse(Double.NaN);
    }
}
