package com.spotify.heroic.analytics.bigtable;

import com.google.api.client.util.Charsets;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.MetricKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class SeriesKeyFilterEncodingTest {
    private final SeriesKeyEncoding foo = new SeriesKeyEncoding("foo");

    @Mock
    MetricKey metricKey;

    final LocalDate date = LocalDate.parse("2016-01-31");

    @Test
    public void testKeyEncoding() throws Exception {
        final ByteString bytes =
            foo.encode(new SeriesKeyEncoding.SeriesKey(date, metricKey), Object::toString);

        assertEquals(ByteString.copyFrom("foo/2016-01-31/metricKey", Charsets.UTF_8), bytes);

        final SeriesKeyEncoding.SeriesKey k = foo.decode(bytes, s -> metricKey);

        assertEquals(date, k.getDate());
        assertEquals(metricKey, k.getKey());

        assertEquals(ByteString.copyFrom("foo/2016-02-01", Charsets.UTF_8),
            foo.rangeKey(date.plusDays(1)));
    }
}
