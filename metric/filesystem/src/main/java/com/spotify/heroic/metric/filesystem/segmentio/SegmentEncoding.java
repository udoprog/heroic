package com.spotify.heroic.metric.filesystem.segmentio;

import com.spotify.heroic.metric.filesystem.SegmentHeader;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentIterable;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface SegmentEncoding {
    /**
     * Write the header.
     */
    void writeHeader(WritableByteChannel channel, SegmentHeader header) throws Exception;

    /**
     * Read segment header.
     */
    SegmentHeader readHeader(ReadableByteChannel channel) throws Exception;

    /**
     * Write points to the given channel.
     */
    void writePoints(WritableByteChannel channel, SegmentIterable<SegmentPoint> points)
        throws Exception;

    /**
     * Write events to the given channel.
     */
    void writeEvents(WritableByteChannel channel, SegmentIterable<SegmentEvent> events)
        throws Exception;

    /**
     * Read points from the given buffer.
     */
    SegmentIterator<SegmentPoint> readPoints(ByteBuffer byteBuffer) throws Exception;

    /**
     * Read events from the given buffer.
     */
    SegmentIterator<SegmentEvent> readEvents(ByteBuffer byteBuffer) throws Exception;

    /**
     * Modify the file name for the given I/O method.
     */
    String applyFileName(final String name);
}
