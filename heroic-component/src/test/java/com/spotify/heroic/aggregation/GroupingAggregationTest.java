package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.spotify.heroic.aggregation.AggregationState.forSeries;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class GroupingAggregationTest {
    static AsyncFramework async = TinyAsync.builder().build();

    @Mock
    Observable<MetricCollection> observable;

    @Mock
    DateRange range;

    @Mock
    Duration duration;

    @Test
    public void testGroup() throws Exception {
        final GroupingAggregation a =
            new Group(Optional.of(ImmutableList.of("site")), Optional.empty());

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "a"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "b"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon", "host", "b"));
        final Series s4 = Series.of("foo", ImmutableMap.of("host", "c"));

        final List<AggregationState> states = Stream
            .of(s1, s2, s3, s4)
            .map(s -> forSeries(s, observable))
            .collect(Collectors.toList());

        final AggregationContext context = AggregationContext.of(async, states, range, duration);

        final AggregationContext out = a.setup(context).get();

        final List<Map<String, String>> groups =
            out.input().stream().map(s -> s.getKey()).collect(Collectors.toList());

        final List<List<Series>> series = out
            .input()
            .stream()
            .map(s -> ImmutableList.copyOf(s.getSeries()))
            .collect(Collectors.toList());

        final List<Map<String, String>> expectedGroups =
            ImmutableList.of(ImmutableMap.of(), ImmutableMap.of("site", "lon"),
                ImmutableMap.of("site", "sto"), ImmutableMap.of("site", "sto"));

        final List<Iterable<Series>> expectedSeries =
            ImmutableList.of(ImmutableList.of(s4), ImmutableList.of(s3), ImmutableList.of(s1),
                ImmutableList.of(s2));

        assertEquals(expectedGroups, groups);
        assertEquals(expectedSeries, series);
    }
}
