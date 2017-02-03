package com.spotify.heroic.metric.filesystem.segmentio;

import com.spotify.heroic.metric.filesystem.SegmentHeader;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentIterable;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import lombok.RequiredArgsConstructor;

/**
 * Simplified but large segment I/O.
 */
@RequiredArgsConstructor
public class BasicSegmentEncoding implements SegmentEncoding {
    private final SerializerFramework serializer;
    private final Serializer<SegmentHeader> segmentHeader;
    private final Serializer<SegmentPoint> points;
    private final Serializer<SegmentEvent> events;

    @Override
    public void writeHeader(
        final WritableByteChannel channel, final SegmentHeader header
    ) throws Exception {
        final SerialWriter buffer = serializer.writeByteChannel(channel);
        segmentHeader.serialize(buffer, header);
    }

    @Override
    public SegmentHeader readHeader(final ReadableByteChannel channel) throws Exception {
        final SerialReader buffer = serializer.readByteChannel(channel);
        return segmentHeader.deserialize(buffer);
    }

    @Override
    public void writePoints(
        final WritableByteChannel channel, final SegmentIterable<SegmentPoint> points
    ) throws Exception {
        final SerialWriter buffer = serializer.writeByteChannel(channel);

        try (final SegmentIterator<SegmentPoint> it = points.iterator()) {
            while (it.hasNext()) {
                this.points.serialize(buffer, it.next());
            }
        }
    }

    @Override
    public void writeEvents(
        final WritableByteChannel channel, final SegmentIterable<SegmentEvent> events
    ) throws Exception {
        final SerialWriter buffer = serializer.writeByteChannel(channel);

        try (final SegmentIterator<SegmentEvent> it = events.iterator()) {
            while (it.hasNext()) {
                this.events.serialize(buffer, it.next());
            }
        }
    }

    @Override
    public SegmentIterator<SegmentPoint> readPoints(final ByteBuffer byteBuffer) throws Exception {
        final long size = byteBuffer.remaining();
        final SerialReader buffer = serializer.readByteBuffer(byteBuffer);

        return new SegmentIterator<SegmentPoint>() {
            @Override
            public boolean hasNext() throws IOException {
                return buffer.position() < size;
            }

            @Override
            public SegmentPoint next() throws IOException {
                return points.deserialize(buffer);
            }

            @Override
            public void close() throws Exception {
                buffer.close();
            }
        };
    }

    @Override
    public SegmentIterator<SegmentEvent> readEvents(final ByteBuffer byteBuffer) throws Exception {
        final long size = byteBuffer.remaining();
        final SerialReader buffer = serializer.readByteBuffer(byteBuffer);

        return new SegmentIterator<SegmentEvent>() {
            @Override
            public boolean hasNext() throws IOException {
                return buffer.position() < size;
            }

            @Override
            public SegmentEvent next() throws IOException {
                return events.deserialize(buffer);
            }

            @Override
            public void close() throws Exception {
                buffer.close();
            }
        };
    }

    @Override
    public String applyFileName(final String name) {
        return name;
    }
}
