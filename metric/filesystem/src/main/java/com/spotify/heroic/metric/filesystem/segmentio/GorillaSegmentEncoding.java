package com.spotify.heroic.metric.filesystem.segmentio;

import com.spotify.heroic.metric.filesystem.SegmentHeader;
import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentIterable;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;
import fi.iki.yak.ts.compression.gorilla.Decompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import lombok.RequiredArgsConstructor;

/**
 * Segment I/O compressed with Gorilla compression for points.
 */
@RequiredArgsConstructor
public class GorillaSegmentEncoding implements SegmentEncoding {
    private final SegmentEncoding delegate;

    @Override
    public SegmentIterator<SegmentPoint> readPoints(final ByteBuffer byteBuffer) {
        final ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        final Decompressor d = new Decompressor(input);

        return new SegmentIterator<SegmentPoint>() {
            private Pair pair = null;

            @Override
            public boolean hasNext() throws IOException {
                flushNext();
                return pair != null;
            }

            @Override
            public SegmentPoint next() throws IOException {
                flushNext();

                final Pair p = this.pair;

                if (p == null) {
                    throw new NoSuchElementException();
                }

                pair = null;
                return new SegmentPoint(p.getTimestamp(), p.getDoubleValue());
            }

            private void flushNext() {
                if (pair == null) {
                    pair = d.readPair();
                }
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    @Override
    public SegmentIterator<SegmentEvent> readEvents(final ByteBuffer byteBuffer) throws Exception {
        return delegate.readEvents(byteBuffer);
    }

    @Override
    public void writeHeader(
        final WritableByteChannel channel, final SegmentHeader header
    ) throws Exception {
        delegate.writeHeader(channel, header);
    }

    @Override
    public SegmentHeader readHeader(final ReadableByteChannel channel) throws Exception {
        return delegate.readHeader(channel);
    }

    @Override
    public void writePoints(
        final WritableByteChannel channel, final SegmentIterable<SegmentPoint> points
    ) throws Exception {
        try (final SegmentIterator<SegmentPoint> it = points.iterator()) {
            if (!it.hasNext()) {
                return;
            }

            final SegmentPoint first = it.next();

            final ByteBufferBitOutput output = new ByteBufferBitOutput();
            final Compressor c = new Compressor(first.getTimestamp(), output);

            c.addValue(first.getTimestamp(), first.getValue());

            while (it.hasNext()) {
                final SegmentPoint p = it.next();
                c.addValue(p.getTimestamp(), p.getValue());
            }

            c.close();

            final ByteBuffer buffer = output.getByteBuffer();
            buffer.flip();

            while (buffer.remaining() > 0) {
                channel.write(buffer);
            }
        }
    }

    @Override
    public void writeEvents(
        final WritableByteChannel channel, final SegmentIterable<SegmentEvent> events
    ) throws Exception {
        delegate.writeEvents(channel, events);
    }

    @Override
    public String applyFileName(final String name) {
        return name + ".tsc";
    }
}
