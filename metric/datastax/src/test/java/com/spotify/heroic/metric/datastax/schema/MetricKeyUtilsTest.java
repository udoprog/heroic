package com.spotify.heroic.metric.datastax.schema;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.MetricKey;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class MetricKeyUtilsTest {
    @Mock
    TypeSerializer<MetricsRowKey> serializer;

    @Mock
    BackendKey key;

    @Mock
    ByteBuffer serializedKey;

    @Mock
    SchemaInstance schema;

    @Mock
    MetricKey metricKey;

    @Before
    public void setup() throws IOException {
        doReturn(serializedKey).when(serializer).serialize(any(MetricsRowKey.class));
        doReturn(serializer).when(schema).rowKey();
        doReturn(metricKey).when(key).toMetricKey();
        doReturn(Optional.of("key")).when(metricKey).getKey();
    }

    @Test
    public void testFloatToToken() {
        assertEquals(Long.MAX_VALUE, MetricKeyUtils.percentageToToken(1.0f));
        assertEquals(Long.MIN_VALUE, MetricKeyUtils.percentageToToken(0.0f));
        assertEquals(0, MetricKeyUtils.percentageToToken(0.5f));
    }

    @Test
    public void testClause() throws Exception {
        final MetricKeyUtils utils = new MetricKeyUtils("key", "keyspace", "table", schema);

        final String base = "SELECT DISTINCT key, token(key) FROM keyspace.table";

        // basic, select everything
        assertEquals(new SchemaBoundStatement(base, ImmutableList.of()),
            utils.selectKeys(BackendKeyFilter.of()));

        // one criteria
        assertEquals(new SchemaBoundStatement(base + " WHERE token(key) >= token(?)",
                ImmutableList.of(serializedKey)),
            utils.selectKeys(BackendKeyFilter.of().withStart(BackendKeyFilter.gte(key))));

        // more criteria
        assertEquals(
            new SchemaBoundStatement(base + " WHERE token(key) >= token(?) and token(key) < ?",
                ImmutableList.of(serializedKey, 42L)), utils.selectKeys(BackendKeyFilter
                .of()
                .withStart(BackendKeyFilter.gte(key))
                .withEnd(BackendKeyFilter.ltToken(42L))));
    }
}
