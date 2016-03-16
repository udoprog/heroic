package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * Tests for aggregation archetypes.
 *
 * @author udoprog
 */
@RunWith(MockitoJUnitRunner.class)
public class AggregationTest {
    static AsyncFramework async = TinyAsync.builder().build();

    @Mock
    DateRange range;

    @Mock
    Duration duration;

    @Mock
    Series s;

    @Test
    public void testTagsElision() throws Exception {
        final GroupingAggregation a =
            new Group(Optional.of(ImmutableList.of("site")), Optional.empty());
        final GroupingAggregation b =
            new Group(Optional.of(ImmutableList.of("host")), Optional.empty());
        final Aggregation chain = Aggregations.chain(a, b);

        assertEquals(ImmutableSet.of("site", "host"), chain.requiredTags());

        final AggregationContext context = AggregationContext.tracing(async,
            ImmutableList.of(AggregationState.forSeries(s, Observable.empty())), range, duration,
            Function.identity());

        final AggregationContext out = chain.setup(context).get();

        assertEquals(ImmutableSet.of("host"), out.requiredTags());
        assertEquals(ImmutableSet.of("site", "host"),
            out.parents().get(0).parents().get(0).parents().get(0).requiredTags());
    }
}
