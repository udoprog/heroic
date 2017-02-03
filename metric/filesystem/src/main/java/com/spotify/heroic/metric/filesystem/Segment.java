package com.spotify.heroic.metric.filesystem;

import com.spotify.heroic.function.ThrowingBiFunction;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.segmentio.SegmentEncoding;
import com.spotify.heroic.metric.filesystem.wal.Wal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import lombok.Data;

@Data
public class Segment<T extends Comparable<T>> {
    private final SegmentEncoding segmentEncoding;
    private final Path segmentPath;
    private final SegmentHeader header;
    private final long headerSize;
    private final ThrowingBiFunction<SegmentEncoding, ByteBuffer, SegmentIterator<T>> reader;

    private final Object lock = new Object();

    private volatile FileChannel file = null;
    private volatile int fileRefCount = 0;

    public SegmentIterator<T> open() throws Exception {
        synchronized (lock) {
            if (fileRefCount == 0) {
                file = (FileChannel) Files.newByteChannel(segmentPath,
                    EnumSet.of(StandardOpenOption.READ));
            }

            fileRefCount++;
        }

        return reader.apply(segmentEncoding, openByteBuffer()).closing(() -> {
            synchronized (lock) {
                final int count = --fileRefCount;

                if (count == 0) {
                    Segment.this.file.close();
                    Segment.this.file = null;
                }
            }
        });
    }

    public boolean isNewer(final long txId) {
        return txId == Wal.DISABLED_TXID || header.getTxId() >= txId;
    }

    private MappedByteBuffer openByteBuffer() throws IOException {
        final MappedByteBuffer opened = file.map(FileChannel.MapMode.READ_ONLY, 0, file.size());
        opened.position((int) headerSize);
        return opened;
    }
}
