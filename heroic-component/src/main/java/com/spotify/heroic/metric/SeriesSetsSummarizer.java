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

package com.spotify.heroic.metric;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.common.Series;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Data;

@Data
public class SeriesSetsSummarizer {
    private HashSet<String> keys;
    private Multimap<String, String> tags;
    private int countOfSets;
    private int seriesSizeMin;
    private int seriesSizeMax;
    private long seriesSizeTotalSum;

    public SeriesSetsSummarizer() {
        keys = new HashSet<String>();
        tags = HashMultimap.create();
        countOfSets = 0;
        seriesSizeMin = Integer.MAX_VALUE;
        seriesSizeMax = Integer.MIN_VALUE;
        seriesSizeTotalSum = 0;
    }

    public void add(Set<Series> seriesSet) {
        if (seriesSet.size() == 0) {
            return;
        }

        for (Series s : seriesSet) {
            keys.add(s.getKey());
            for (Map.Entry<String, String> e : s.getTags().entrySet()) {
                tags.put(e.getKey(), e.getValue());
            }
        }

        countOfSets++;
        int dataSize = seriesSet.size();
        seriesSizeTotalSum += dataSize;
        if (dataSize < seriesSizeMin) {
            seriesSizeMin = dataSize;
        }
        if (dataSize > seriesSizeMax) {
            seriesSizeMax = dataSize;
        }
    }

    public Summary end() {
        Optional<Integer> min = Optional.empty();
        Optional<Integer> max = Optional.empty();
        Optional<Integer> mean = Optional.empty();
        Optional<Long> totalSum = Optional.empty();
        if (countOfSets != 0) {
            min = Optional.of(seriesSizeMin);
            max = Optional.of(seriesSizeMax);
            mean = Optional.of((int) (seriesSizeTotalSum / (long) countOfSets));
            totalSum = Optional.of(seriesSizeTotalSum);
        }
        return new Summary(new ArrayList<String>(keys), tags, min, max, mean, totalSum);
    }

    @Data
    public class Summary {
        final List<String> keys;
        final Multimap<String, String> tags;
        final Optional<Integer> seriesSizeMin;
        final Optional<Integer> seriesSizeMax;
        final Optional<Integer> seriesSizeMean;
        final Optional<Long> seriesSizeTotalSum;
    }
}
