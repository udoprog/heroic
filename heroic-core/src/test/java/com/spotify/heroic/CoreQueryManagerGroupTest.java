package com.spotify.heroic;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import eu.toolchain.async.AsyncFramework;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CoreQueryManagerGroupTest {
    private CoreQueryManagerGroup group;

    private final Set<String> features = ImmutableSet.of();

    @Mock
    private AsyncFramework async;

    @Mock
    private ClusterManager cluster;

    @Mock
    private FilterFactory filters;

    @Mock
    private AggregationFactory aggregations;

    @Mock
    private QueryParser parser;

    @Mock
    private QueryCache queryCache;

    @Mock
    private Iterable<ClusterNode.Group> groups;

    @Before
    public void setup() {
        group = new CoreQueryManagerGroup(async, filters, aggregations, queryCache, features, null,
            groups);
    }

    @Test
    public void testEndRangeIsNow() {
        final DateRange range = DateRange.create(50_000L, 150_000L);

        final DateRange shiftedRange = group.buildShiftedRange(range, 5_000, 150_000L);

        assertEquals(DateRange.create(50_000L, 140_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsTooCloseToNow() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        final DateRange shiftedRange = group.buildShiftedRange(range, 5_000, 155_000L);

        assertEquals(DateRange.create(50_000L, 145_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsOk() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        final DateRange shiftedRange = group.buildShiftedRange(range, 5_000, 184_000L);

        assertEquals(DateRange.create(50_000L, 155_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsInTheFuture() {
        final DateRange range = DateRange.create(50_000L, 180_000L);

        final DateRange shiftedRange = group.buildShiftedRange(range, 5_000, 150_000L);

        assertEquals(DateRange.create(50_000L, 140_000L), shiftedRange);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartRangeIsInTheFuture() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        group.buildShiftedRange(range, 5_000, 40_000L);
    }
}
