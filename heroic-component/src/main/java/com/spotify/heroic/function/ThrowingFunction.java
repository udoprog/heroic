package com.spotify.heroic.function;

@FunctionalInterface
public interface ThrowingFunction<I, O> {
    O apply(I input) throws Exception;
}
