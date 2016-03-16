package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.metric.MetricCollection;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AggregationDataTest {
    private final ObjectMapper m = HeroicMappers.json();

    @Test
    public void testSerialization() throws Exception {
        final AggregationData d =
            new AggregationData(ImmutableMap.of(), Iterables.concat(ImmutableList.of()),
                MetricCollection.empty());

        final AggregationData r = m.readValue(m.writeValueAsString(d), AggregationData.class);

        assertEquals(d.getKey(), r.getKey());
        assertEquals(ImmutableList.copyOf(d.getSeries()), ImmutableList.copyOf(r.getSeries()));
        assertEquals(d.getMetrics(), r.getMetrics());
    }
}
