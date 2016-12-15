package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.metric.CompositeCollection;
import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class AggregationOutputTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(AggregationOutput.class);
    }

    @Test
    public void isEmpty() {
        final AggregationOutput output = new AggregationOutput(ImmutableMap.of(), ImmutableSet.of(),
            new CompositeCollection.Points(ImmutableList.of()));

        assertTrue(output.isEmpty());
    }

    @Test
    public void withKey() {
        final AggregationOutput output = new AggregationOutput(ImmutableMap.of(), ImmutableSet.of(),
            new CompositeCollection.Points(ImmutableList.of()));

        final Map<String, String> key = ImmutableMap.of("key", "value");

        final AggregationOutput next = output.withKey(key);

        assertTrue(output.isEmpty());
        assertNotSame(output, next);
        assertEquals(key, next.getKey());
    }
}
