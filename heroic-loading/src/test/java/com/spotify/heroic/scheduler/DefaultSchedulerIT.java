package com.spotify.heroic.scheduler;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchedulerIT {
    private DeterministicScheduler deterministic;
    private DefaultScheduler scheduler;

    @Mock
    public AsyncFramework async;
    @Mock
    public AsyncFuture<Void> f1;
    @Mock
    public AsyncFuture<Void> f2;

    @Before
    public void setup() {
        deterministic = new DeterministicScheduler();
        scheduler = new DefaultScheduler(async, deterministic);

        doAnswer(invocation -> {
            final FutureFinished runnable = invocation.getArgumentAt(0, FutureFinished.class);
            runnable.finished();
            return null;
        }).when(f1).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testUnique() {
        final UniqueTaskHandle foo = scheduler.unique("foo");

        foo.schedule(10, TimeUnit.SECONDS, () -> f1);
        foo.schedule(10, TimeUnit.SECONDS, () -> f2);

        deterministic.tick(19, TimeUnit.SECONDS);

        verify(f1).onFinished(any(FutureFinished.class));
        verify(f2, never()).onFinished(any(FutureFinished.class));

        deterministic.tick(1, TimeUnit.SECONDS);

        verify(f1).onFinished(any(FutureFinished.class));
        verify(f2).onFinished(any(FutureFinished.class));
    }
}
