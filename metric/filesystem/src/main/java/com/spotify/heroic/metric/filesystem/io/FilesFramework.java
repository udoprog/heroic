package com.spotify.heroic.metric.filesystem.io;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(RealFilesFramework.class)
})
public interface FilesFramework {
    FileChannel newFileChannel(Path path, OpenOption... options) throws IOException;

    FileChannel newFileChannel(Path path, EnumSet<StandardOpenOption> options) throws IOException;

    void move(Path source, Path target, CopyOption... options) throws IOException;

    void delete(Path path) throws IOException;

    boolean isDirectory(Path path, LinkOption... options);

    void createDirectory(Path rootPath, FileAttribute<?>... attributes) throws IOException;

    long size(Path path) throws IOException;

    DirectoryStream<Path> newDirectoryStream(Path path) throws IOException;
}
