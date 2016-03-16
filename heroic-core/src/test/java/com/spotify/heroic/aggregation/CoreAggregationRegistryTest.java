package com.spotify.heroic.aggregation;

import eu.toolchain.serializer.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CoreAggregationRegistryTest {
    @Mock
    Serializer<String> string;

    @Mock
    Function<AggregationArguments, Aggregation> dsl;

    CoreAggregationRegistry registry;

    @Before
    public void setup() {
        registry = new CoreAggregationRegistry(string);
    }

    @Test
    public void testRegisterQuery() {
        registry.register("foo", A.class, dsl);

        assertEquals("foo", registry.definitionMap.get(A.class));
        assertEquals(dsl, registry.builderMap.get("foo"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameId() {
        registry.register("foo", A.class, dsl);
        registry.register("foo", B.class, dsl);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameAggregation() {
        registry.register("foo", A.class, dsl);
        registry.register("bar", A.class, dsl);
    }

    interface A extends Aggregation {
    }

    interface B extends Aggregation {
    }
}
