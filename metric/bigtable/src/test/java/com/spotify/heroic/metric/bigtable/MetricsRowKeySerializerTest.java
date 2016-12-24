package com.spotify.heroic.metric.bigtable;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricKey;
import eu.toolchain.serializer.BytesSerialWriter;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import junit.framework.TestCase;
import org.junit.Assert;

public class MetricsRowKeySerializerTest extends TestCase {
    private MetricKey key =
        MetricKey.of(Series.of("key", ImmutableMap.of("from", "123", "to", "4567")));

    private static final String EXPECTED_SERIALIZATION =
        "\u0003\u0003key" + "\u0002\u0004\u0004from\u0003\u0003123" +
            "\u0002\u0002to\u0004\u00044567" + "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001";

    private final SerializerFramework serializerFramework = TinySerializer.builder().build();
    private final Serializer<RowKey> serializer = new MetricsRowKeySerializer();

    public void testSerializationIsBackwardsCompatible() throws Exception {
        final RowKey rowKey = RowKey.of(key, 1);

        final BytesSerialWriter serializedBytes = serializerFramework.writeBytes();
        serializer.serialize(serializedBytes, rowKey);

        Assert.assertArrayEquals(EXPECTED_SERIALIZATION.getBytes(), serializedBytes.toByteArray());
    }

    public void testDeserializationIsBackwardsCompatible() throws Exception {
        final SerialReader serialReader =
            serializerFramework.readByteArray(EXPECTED_SERIALIZATION.getBytes());

        final RowKey rowKey = serializer.deserialize(serialReader);

        assertEquals(rowKey, RowKey.of(key, 1));
    }
}
