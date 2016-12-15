package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.CompositeCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterKAreaStrategyTest {
    @Test
    public void testNoDatapoints() {
        final List<FilterableData<Integer>> metrics = Collections.singletonList(
            new FilterableData<>(42, new CompositeCollection.Points(Collections.emptyList())));
        final FilterKAreaStrategy filter = new FilterKAreaStrategy(FilterKAreaType.BOTTOM, 1);

        // We expect timeseries with 0 datapoints to be filtered away
        assertEquals(0, filter.filter(metrics).size());
    }
}
