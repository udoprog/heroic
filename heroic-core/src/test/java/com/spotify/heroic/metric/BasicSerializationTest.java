package com.spotify.heroic.metric;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.HeroicMappers;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class BasicSerializationTest {
    private ObjectMapper mapper = HeroicMappers.json();

    @Test
    public void testEvent() throws Exception {
        final Event expected = new Event(1024L,
            ImmutableMap.<String, Object>of("int", 1234, "float", 0.25d, "string", "foo"));
        assertSerialization("Event.json", expected, Event.class);
    }

    @Test
    public void testPoint() throws Exception {
        final Point expected = new Point(1024L, 3.14);
        assertSerialization("Point.json", expected, Point.class);
    }

    @Test
    public void testMetricCollection() throws Exception {
        final MetricCollection expected = MetricCollection.points(
            ImmutableList.of(new Point(1000, 10.0d), new Point(2000, 20.0d)));
        assertSerialization("MetricCollection.json", expected, MetricCollection.class);
    }

    private <T> void assertSerialization(final String json, final T expected, final Class<T> type)
        throws IOException, JsonParseException, JsonMappingException {
        // verify that it is equal to the local file.
        try (InputStream in = openResource(json)) {
            assertEquals(expected, mapper.readValue(in, type));
        }

        // roundtrip
        final String string = mapper.writeValueAsString(expected);
        assertEquals(expected, mapper.readValue(string, type));
    }

    private InputStream openResource(String path) {
        final Class<?> cls = BasicSerializationTest.class;
        final String fullPath = cls.getPackage().getName().replace('.', '/') + "/" + path;
        return cls.getClassLoader().getResourceAsStream(fullPath);
    }
}
