package com.spotify.heroic.metric.disk;

import com.google.common.io.Files;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.Executors;

public class CommitLogIT {
    private AsyncFramework async;
    private Path root;

    private final CommitLog.LogEntry e1 = new CommitLog.LogEntry(new byte[]{1}, 1, new byte[]{1});
    private final CommitLog.LogEntry e2 = new CommitLog.LogEntry(new byte[]{2}, 2, new byte[]{2});

    private CommitLog log;

    @Before
    public void setup() throws Exception {
        async = TinyAsync.builder().executor(Executors.newSingleThreadExecutor()).build();
        root = Files.createTempDir().toPath();
        log = new CommitLog(async, root);
        log.start().get();
    }

    @After
    public void teardown() throws Exception {
        log.stop().get();
    }

    @Test
    public void testNothing() throws Exception {
        System.out.println(log.index.get());
        System.out.println("Temp: " + root);
        log.write(e1).get();
        log.write(e2).get();
    }
}
