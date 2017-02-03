package com.spotify.heroic.function;

@FunctionalInterface
public interface ThrowingBiFunction<A, B, O> {
    O apply(A a, B b) throws Exception;
}
