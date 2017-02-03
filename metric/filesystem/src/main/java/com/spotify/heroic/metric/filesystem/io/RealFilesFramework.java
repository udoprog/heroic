package com.spotify.heroic.metric.filesystem.io;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

@JsonTypeName("real")
public class RealFilesFramework implements FilesFramework {
    @Override
    public FileChannel newFileChannel(final Path path, final OpenOption... options)
        throws IOException {
        return FileChannel.open(path, options);
    }

    @Override
    public FileChannel newFileChannel(
        final Path path, final EnumSet<StandardOpenOption> options
    ) throws IOException {
        return FileChannel.open(path, options);
    }

    @Override
    public void move(final Path source, final Path target, final CopyOption... options)
        throws IOException {
        Files.move(source, target, options);
    }

    @Override
    public void delete(final Path path) throws IOException {
        Files.delete(path);
    }

    @Override
    public boolean isDirectory(final Path path, final LinkOption... options) {
        return Files.isDirectory(path, options);
    }

    @Override
    public void createDirectory(final Path path, final FileAttribute<?>... attributes)
        throws IOException {
        Files.createDirectory(path, attributes);
    }

    @Override
    public long size(final Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir) throws IOException {
        return Files.newDirectoryStream(dir);
    }
}
