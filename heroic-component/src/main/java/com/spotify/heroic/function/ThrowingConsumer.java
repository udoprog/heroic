package com.spotify.heroic.function;

@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T value) throws Exception;
}
