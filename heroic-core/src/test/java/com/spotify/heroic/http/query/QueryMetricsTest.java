package com.spotify.heroic.http.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.aggregation.Group;
import com.spotify.heroic.common.TypeNameMixin;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class QueryMetricsTest {
    public InputStream resource(final String name) {
        return getClass()
            .getClassLoader()
            .getResourceAsStream(getClass().getPackage().getName().replace('.', '/') + '/' + name);
    }

    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();
        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);
        mapper.registerModule(new Jdk8Module());
        mapper.registerSubtypes(new NamedType(Group.class, Group.NAME));
        mapper.registerSubtypes(new NamedType(Empty.class, Empty.NAME));
    }

    private QueryMetrics parse(final String name) throws Exception {
        try (final InputStream in = resource(name)) {
            return mapper.readValue(in, QueryMetrics.class);
        }
    }

    @Test
    public void testAggregationCompatibility() throws Exception {
        final QueryMetrics a = parse("QueryMetrics.AggregationCompat.1.json");
        final QueryMetrics b = parse("QueryMetrics.AggregationCompat.2.json");
        final QueryMetrics c = parse("QueryMetrics.AggregationCompat.3.json");

        assertEquals(a, b);
        assertEquals(a, c);
    }
}
