package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MetricCollectionTest {
    private static final Class<? extends Metric> TYPE_A = Point.class;
    private static final Class<? extends Metric> TYPE_B = Event.class;
    private static final Map<String, String> TAGS = ImmutableMap.of();
    private static final Set<Series> SERIES = ImmutableSet.of();

    @Mock
    public Point p1;
    @Mock
    public Point p2;

    @Mock
    public Event e1;
    @Mock
    public Event e2;

    @Mock
    public Spread s1;
    @Mock
    public Spread s2;

    @Mock
    public MetricGroup g1;
    @Mock
    public MetricGroup g2;

    @Mock
    public Payload pay1;
    @Mock
    public Payload pay2;

    @Test
    public void testPoints() {
        final ImmutableList<Point> data = ImmutableList.of(p1, p2);
        final MetricCollection points = MetricCollection.points(data);

        testCollection(data, points, Point.class, MetricType.POINT, session -> {
            verify(session).updatePoints(TAGS, SERIES, data);
        });
    }

    @Test
    public void testEvents() {
        final ImmutableList<Event> data = ImmutableList.of(e1, e2);
        final MetricCollection events = MetricCollection.events(data);

        testCollection(data, events, Event.class, MetricType.EVENT, session -> {
            verify(session).updateEvents(TAGS, SERIES, data);
        });
    }

    @Test
    public void testSpreads() {
        final ImmutableList<Spread> data = ImmutableList.of(s1, s2);
        final MetricCollection spreads = MetricCollection.spreads(data);

        testCollection(data, spreads, Spread.class, MetricType.SPREAD, session -> {
            verify(session).updateSpreads(TAGS, SERIES, data);
        });
    }

    @Test
    public void testGroups() {
        final ImmutableList<MetricGroup> data = ImmutableList.of(g1, g2);
        final MetricCollection groups = MetricCollection.groups(data);

        testCollection(data, groups, MetricGroup.class, MetricType.GROUP, session -> {
            verify(session).updateGroup(TAGS, SERIES, data);
        });
    }

    @Test
    public void testCardinalities() {
        final ImmutableList<Payload> data = ImmutableList.of(pay1, pay2);
        final MetricCollection cardinalities = MetricCollection.cardinality(data);

        testCollection(data, cardinalities, Payload.class, MetricType.CARDINALITY, session -> {
            verify(session).updatePayload(TAGS, SERIES, data);
        });
    }

    @Test
    public void testEmpty() {
        assertEquals(MetricCollection.points(ImmutableList.of()), MetricCollection.empty());
    }

    @Test
    public void testMergeSorted() {
        final List<Point> a = ImmutableList.of(new Point(10, 1.0), new Point(20, 1.0));
        final List<Point> b = ImmutableList.of(new Point(5, 2.0), new Point(20, 2.0));
        final MetricCollection collection =
            MetricCollection.mergeSorted(MetricType.POINT, ImmutableList.of(a, b));

        // NOTE: this does permit duplicates
        assertEquals(ImmutableList.of(new Point(5, 2.0), new Point(10, 1.0), new Point(20, 2.0),
            new Point(20, 1.0)), collection.getData());
    }

    private <T> void testCollection(
        final ImmutableList<T> data, final MetricCollection collection, final Class<T> expected,
        final MetricType expectedType, final Consumer<AggregationSession> expectSession
    ) {
        final Class<? extends Metric> other = TYPE_A == expected ? TYPE_B : TYPE_A;

        assertEquals(expectedType, collection.getType());
        assertEquals(data, collection.getDataAs(expected));
        assertEquals(Optional.of(data), collection.getDataOptional(expected));
        assertEquals(Optional.empty(), collection.getDataOptional(other));
        assertEquals(data, collection.getData());
        assertEquals(data, collection.getDataUnsafe());

        assertEquals(collection, MetricCollection.build(expectedType, collection.getData()));
        assertFalse(collection.isEmpty());

        try {
            collection.getDataAs(other);
            fail("should not support getting data as other type");
        } catch (final IllegalArgumentException e) {
            assertEquals(
                "expected (" + other.getCanonicalName() + ") but is (" + expectedType + ")",
                e.getMessage());
        }

        final AggregationSession session = mock(AggregationSession.class);
        collection.updateAggregation(session, TAGS, SERIES);
        expectSession.accept(session);
    }
}
