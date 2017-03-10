package com.spotify.heroic.metric.filesystem;

import com.spotify.heroic.function.ThrowingBiFunction;
import com.spotify.heroic.metric.filesystem.io.SegmentIterator;
import com.spotify.heroic.metric.filesystem.segmentio.SegmentEncoding;
import com.spotify.heroic.metric.filesystem.wal.Wal;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

@Data
@Slf4j
public class Segment<T extends Comparable<T>> {
    private final SegmentEncoding segmentEncoding;
    private final Path segmentPath;
    private final SegmentHeader header;
    private final long headerSize;
    private final ThrowingBiFunction<SegmentEncoding, ByteBuffer, SegmentIterator<T>> reader;

    private final Object lock = new Object();

    private volatile FileChannel file = null;
    private volatile MappedByteBuffer mapped = null;
    private volatile int refCount = 0;

    public SegmentIterator<T> open() throws Exception {
        /* guarantee a valid mapped location after open */
        synchronized (lock) {
            if (refCount++ == 0) {
                file = (FileChannel) Files.newByteChannel(segmentPath,
                    EnumSet.of(StandardOpenOption.READ));

                final MappedByteBuffer opened =
                    file.map(FileChannel.MapMode.READ_ONLY, 0, file.size());
                opened.position((int) headerSize);
                mapped = opened;
            }
        }

        return reader.apply(segmentEncoding, mapped.asReadOnlyBuffer()).closing(() -> {
            final FileChannel file;
            final MappedByteBuffer mapped;

            synchronized (lock) {
                if (--refCount != 0) {
                    return;
                }

                file = this.file;
                mapped = this.mapped;

                this.file = null;
                this.mapped = null;
            }

            if (mapped instanceof DirectBuffer) {
                ((DirectBuffer) mapped).cleaner().clean();
            }

            file.close();
        });
    }

    public boolean isNewer(final long txId) {
        return txId == Wal.DISABLED_TXID || header.getTxId() >= txId;
    }
}
