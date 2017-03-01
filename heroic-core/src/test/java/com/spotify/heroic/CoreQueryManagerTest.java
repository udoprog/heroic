package com.spotify.heroic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.time.Clock;
import eu.toolchain.async.AsyncFramework;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CoreQueryManagerTest {
    private CoreQueryManager manager;

    @Mock
    private AsyncFramework async;

    @Mock
    private ClusterManager cluster;

    @Mock
    private QueryParser parser;

    @Mock
    private QueryCache queryCache;

    @Mock
    private AggregationFactory aggregations;

    @Before
    public void setup() {
        QueryReporter queryReporter = mock(QueryReporter.class);
        long smallQueryThreshold = 0;

        QueryLogger queryLogger = mock(QueryLogger.class);
        QueryLoggerFactory queryLoggerFactory = mock(QueryLoggerFactory.class);
        when(queryLoggerFactory.create(any())).thenReturn(queryLogger);

        manager = new CoreQueryManager(Features.empty(), async, Clock.system(), cluster, parser,
            queryCache, aggregations, OptionalLimit.empty(), smallQueryThreshold, queryReporter,
            queryLoggerFactory);
    }

    @Test
    public void testBuildQueryRange() {
        final Optional<Long> cadence = Optional.of(10_000L);
        final DateRange range = DateRange.create(40_000L, 50_000L);

        final Function<Long, DateRange> runner =
            (now) -> manager.buildQueryRange(range, cadence, now);

        // end range _is_ now, and is shifted back to within tolerance
        assertEquals(DateRange.create(20_000L, 40_000L), runner.apply(range.getEnd()));
        assertEquals(DateRange.create(10_000L, 30_000L), runner.apply(range.getEnd() - 1));

        // multiple (2) shifts because end range is far into the future.
        assertEquals(DateRange.create(10_000L, 30_000L),
            runner.apply(range.getEnd() - CoreQueryManager.SHIFT_TOLERANCE));

        // end is too close to now, and the entire range is shifted back
        assertEquals(DateRange.create(20_000L, 40_000L),
            runner.apply(range.getEnd() + CoreQueryManager.SHIFT_TOLERANCE - 1));

        // end is within shift tolerance
        assertEquals(DateRange.create(30_000L, 50_000L),
            runner.apply(range.getEnd() + CoreQueryManager.SHIFT_TOLERANCE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartRangeIsInTheFuture() {
        final DateRange range = DateRange.create(50_000L, 50_001L);
        manager.buildQueryRange(range, Optional.of(5_000L), range.getStart());
    }
}
