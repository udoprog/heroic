package com.spotify.heroic.scheduler;

import eu.toolchain.async.AsyncFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handler for a unique task.
 */
public interface UniqueTaskHandle {
    /**
     * Schedule the task to be run.
     *
     * This has no effect if there is already a task scheduled, that is yet to execute.
     *
     * @param value time until the task should run
     * @param unit unit of the value
     * @param task task to run
     */
    void schedule(long value, TimeUnit unit, UniqueTask task);

    /**
     * Make sure the task runs as soon as possible.
     */
    void scheduleNow(UniqueTask task);

    /**
     * Stop the task from running again.
     */
    AsyncFuture<Void> stop();
}
