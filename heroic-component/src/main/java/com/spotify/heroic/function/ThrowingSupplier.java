package com.spotify.heroic.function;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}
