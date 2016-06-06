package com.spotify.heroic.metric.disk;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class CommitLog implements LifeCycles {
    public static final long INITIAL = -1;
    public static final int CHUNK_SIZE = 0xffff;
    public static final int CHUNK_LIMIT = 0xff;

    private static final Comparator<Path> COMPARATOR = (a, b) -> {
        return a.getFileName().compareTo(b.getFileName());
    };

    private static final Base64.Decoder base64d = Base64.getDecoder();
    private static final Base64.Encoder base64e = Base64.getEncoder();

    private final AsyncFramework async;
    private final Path root;

    final AtomicLong index = new AtomicLong(INITIAL);
    final AtomicReferenceArray<Chunk> buffer = new AtomicReferenceArray<>(CHUNK_LIMIT);

    @Inject
    public CommitLog(final AsyncFramework async, final Path root) {
        this.async = async;
        this.root = root;
    }

    @Override
    public void register(final LifeCycleRegistry registry) {
        registry.start(this::start);
    }

    private long decode(final String name) {
        return Longs.fromByteArray(base64d.decode(name));
    }

    private String encode(final long index) {
        return base64e.encodeToString(Longs.toByteArray(index));
    }

    public AsyncFuture<Void> write(LogEntry e) {
        final long w = index.getAndIncrement();

        if (w % CHUNK_SIZE == 0) {
            System.out.println("Create new chunk");
        }

        return async.resolved();
    }

    public AsyncFuture<Void> flush() throws Exception {
        final long position = index.get();
        long end = (position / CHUNK_SIZE);
        final long start = (position / CHUNK_SIZE) - CHUNK_LIMIT;

        while (end >= start) {
            final int here = (int) Math.floorMod(end, CHUNK_LIMIT);
            final long absolute = (end--) * CHUNK_SIZE;

            final Chunk c = buffer.get(here);

            if (c == null) {
                break;
            }

            final Path p = root.resolve(encode(absolute));

            log.info("Flushing: {}", p);

            try (final SerialWriter w = s.writeStream(Files.newOutputStream(p))) {
                chunkS.serialize(w, c);
            }
        }

        return async.resolved();
    }

    public AsyncFuture<Void> load() throws Exception {
        final ImmutableList.Builder<Path> logs = ImmutableList.builder();
        Files.newDirectoryStream(root).forEach(logs::add);
        final List<Path> sorted = Ordering.from(COMPARATOR).sortedCopy(logs.build());

        final long indexPosition =
            sorted.isEmpty() ? 0L : decode(sorted.get(sorted.size() - 1).getFileName().toString());

        for (final Path p : sorted) {
            final long position = decode(p.getFileName().toString());
            final int here = (int) Math.floorMod(position / CHUNK_SIZE, CHUNK_LIMIT);

            log.info("Loading: {}", p);

            final Chunk c;

            try (final SerialReader r = s.readStream(Files.newInputStream(p))) {
                c = chunkS.deserialize(r);
            }

            buffer.compareAndSet(here, null, c);
        }

        if (!index.compareAndSet(INITIAL, indexPosition)) {
            throw new IllegalStateException("Index at unexpected value");
        }

        return async.resolved();
    }

    AsyncFuture<Void> start() {
        return async.call(() -> {
            load();
            return null;
        });
    }

    AsyncFuture<Void> stop() {
        return async.call(() -> {
            flush();
            return null;
        });
    }

    @RequiredArgsConstructor
    public static class Chunk {
        private final LogEntry entries[];
        private final AtomicInteger w;

        Stream<LogEntry> scan(final Function<LogEntry, Boolean> matcher) {
            final int last = w.get();
            final Stream.Builder<LogEntry> result = Stream.builder();

            for (int i = 0; i < last; i++) {
                final LogEntry e = entries[i];

                if (matcher.apply(e)) {
                    result.add(e);
                }
            }

            return result.build();
        }
    }

    @RequiredArgsConstructor
    public static class LogEntry {
        private final byte[] id;
        private final long index;
        private final byte[] value;
    }

    private final SerializerFramework s = TinySerializer.builder().build();

    private final Serializer<LogEntry> leS = new Serializer<LogEntry>() {
        final Serializer<byte[]> byteS = s.byteArray();
        final Serializer<Long> longS = s.fixedLong();

        @Override
        public void serialize(final SerialWriter buffer, final LogEntry value) throws IOException {
            byteS.serialize(buffer, value.id);
            longS.serialize(buffer, value.index);
            byteS.serialize(buffer, value.value);
        }

        @Override
        public LogEntry deserialize(final SerialReader buffer) throws IOException {
            final byte[] id = byteS.deserialize(buffer);
            final long index = longS.deserialize(buffer);
            final byte[] value = byteS.deserialize(buffer);
            return new LogEntry(id, index, value);
        }
    };

    private final Serializer<Chunk> chunkS = new Serializer<Chunk>() {
        private final Serializer<Integer> pS = s.fixedInteger();

        @Override
        public void serialize(
            final SerialWriter buffer, final Chunk value
        ) throws IOException {
            final LogEntry[] entries = value.entries;
            final int position = value.w.get();
            pS.serialize(buffer, position);

            for (int i = 0; i < position; i++) {
                leS.serialize(buffer, entries[i]);
            }
        }

        @Override
        public Chunk deserialize(final SerialReader buffer) throws IOException {
            final LogEntry[] entries = new LogEntry[CHUNK_SIZE];
            final int position = pS.deserialize(buffer);

            for (int i = 0; i < position; i++) {
                entries[i] = leS.deserialize(buffer);
            }

            return new Chunk(entries, new AtomicInteger(position));
        }
    };
}
