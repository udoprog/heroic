package com.spotify.heroic.function;

@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
