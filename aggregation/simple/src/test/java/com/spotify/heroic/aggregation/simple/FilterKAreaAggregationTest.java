package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.GroupInstance;
import com.spotify.heroic.aggregation.GroupingAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterKAreaAggregationTest {

    @Test
    public void testFilterKAreaSession() {
        final GroupingAggregation g =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final TopKInstance t1 = new TopKInstance(2, g);
        final BottomKInstance b1 = new BottomKInstance(1, t1);

        final List<AggregationState> states = new ArrayList<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "ash"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon"));
        final Series s4 = Series.of("foo", ImmutableMap.of("site", "sjc"));

        states.add(AggregationState.forSeries(s1));
        states.add(AggregationState.forSeries(s2));
        states.add(AggregationState.forSeries(s3));
        states.add(AggregationState.forSeries(s4));

        final AggregationSession session = b1.session(states, new DateRange(0, 10000)).getSession();

        session.updatePoints(s1.getTags(), ImmutableList.of(new Point(1, 1.0), new Point(2, 1.0)));
        session.updatePoints(s2.getTags(), ImmutableList.of(new Point(1, 2.0), new Point(2, 2.0)));
        session.updatePoints(s3.getTags(), ImmutableList.of(new Point(1, 3.0), new Point(2, 3.0)));
        session.updatePoints(s4.getTags(), ImmutableList.of(new Point(1, 4.0), new Point(2, 4.0)));

        final List<AggregationData> result = session.result().getResult();

        assertEquals(1, result.size());

        AggregationData first = result.get(0);

        if (first.getGroup().equals(ImmutableMap.of("site", "lon"))) {
            assertEquals(ImmutableList.of(new Point(1, 3.0), new Point(2, 3.0)),
                first.getMetrics().getData());
        } else {
            Assert.fail("unexpected group: " + first.getGroup());
        }
    }
}
