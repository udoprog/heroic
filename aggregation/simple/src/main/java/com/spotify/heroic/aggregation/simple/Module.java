/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationArguments;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.aggregation.Shift;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.ListExpression;
import dagger.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Module implements HeroicModule {
    @Override
    public Entry setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    static class E implements HeroicModule.Entry {
        private final AggregationRegistry c;
        private final AggregationFactory factory;

        @Inject
        public E(
            AggregationRegistry c, AggregationFactory factory
        ) {
            this.c = c;
            this.factory = factory;
        }

        @Override
        public void setup() {
            /* example aggregation */
            c.register(Template.NAME, Template.class, samplingBuilder(Template::new));
            c.register(Spread.NAME, Spread.class, samplingBuilder(Spread::new));
            c.register(Sum.NAME, Sum.class, samplingBuilder(Sum::new));
            c.register(Average.NAME, Average.class, samplingBuilder(Average::new));
            c.register(Min.NAME, Min.class, samplingBuilder(Min::new));
            c.register(Max.NAME, Max.class, samplingBuilder(Max::new));
            c.register(StdDev.NAME, StdDev.class, samplingBuilder(StdDev::new));
            c.register(CountUnique.NAME, CountUnique.class, samplingBuilder(CountUnique::new));
            c.register(Count.NAME, Count.class, samplingBuilder(Count::new));
            c.register(GroupUnique.NAME, GroupUnique.class, samplingBuilder(GroupUnique::new));

            c.register(Quantile.NAME, Quantile.class, new SamplingAggregationDSL<Quantile>() {
                @Override
                protected Quantile buildWith(
                    final AggregationArguments args, final Optional<Duration> size,
                    final Optional<Duration> extent, final Optional<Expression> reference
                ) {
                    final Optional<Double> q = args.getNext("q", Double.class);
                    final Optional<Double> error = args.getNext("error", Double.class);
                    return new Quantile(Optional.empty(), size, extent, reference, q, error);
                }
            });

            c.register(TopK.NAME, TopK.class, new FilterAggregationBuilder<TopK, Long>(Long.class) {
                @Override
                protected TopK build(
                    AggregationArguments args, Long k, Optional<Expression> reference
                ) {
                    return new TopK(k, reference);
                }
            });

            c.register(BottomK.NAME, BottomK.class,
                new FilterAggregationBuilder<BottomK, Long>(Long.class) {
                    @Override
                    protected BottomK build(
                        AggregationArguments args, Long k, Optional<Expression> reference
                    ) {
                        return new BottomK(k, reference);
                    }
                });

            c.register(AboveK.NAME, AboveK.class,
                new FilterAggregationBuilder<AboveK, Double>(Double.class) {
                    @Override
                    protected AboveK build(
                        AggregationArguments args, Double k, Optional<Expression> reference
                    ) {
                        return new AboveK(k, reference);
                    }
                });

            c.register(BelowK.NAME, BelowK.class,
                new FilterAggregationBuilder<BelowK, Double>(Double.class) {
                    @Override
                    protected BelowK build(
                        AggregationArguments args, Double k, Optional<Expression> reference
                    ) {
                        return new BelowK(k, reference);
                    }
                });

            c.register(Subtract.NAME, Subtract.class, biFunction(Subtract::new));
            c.register(Add.NAME, Add.class, biFunction(Add::new));
            c.register(Multiply.NAME, Multiply.class, biFunction(Multiply::new));
            c.register(Divide.NAME, Divide.class, biFunction(Divide::new));
            c.register(Difference.NAME, Difference.class, biFunction(Difference::new));

            c.register(Absolute.NAME, Absolute.class, function(Absolute::new));
            c.register(Negate.NAME, Negate.class, function(Negate::new));

            /* shiftdiff alias */
            c.registerAlias("shiftdiff", in -> {
                final AggregationArguments args = in.arguments();

                final ImmutableList.Builder<Expression> shiftArgsBuilder = ImmutableList.builder();
                args.getNext().ifPresent(shiftArgsBuilder::add);
                args.getNext().ifPresent(shiftArgsBuilder::add);

                args.throwUnlessEmpty("shiftdiff");

                final List<Expression> shiftArgs = shiftArgsBuilder.build();

                final Expression shift =
                    Expression.aggregation(Shift.NAME, Expression.list(shiftArgs),
                        ImmutableMap.of());

                ImmutableList.Builder<Expression> divideArgsBuilder = ImmutableList.builder();
                divideArgsBuilder.add(shift);
                shiftArgs.stream().findFirst().ifPresent(divideArgsBuilder::add);

                final List<Expression> divideArgs = divideArgsBuilder.build();

                final Expression divide =
                    Expression.aggregation(Divide.NAME, Expression.list(divideArgs),
                        ImmutableMap.of());

                final ListExpression subtractArgs = Expression.list(divide, Expression.integer(1));
                return Expression.aggregation(Subtract.NAME, subtractArgs, ImmutableMap.of());
            });
        }

        private Function<AggregationArguments, Aggregation> biFunction(
            final BiFunction<Optional<Expression>, Optional<Expression>, Aggregation> builder
        ) {
            return args -> {
                final Optional<Expression> left = args.getNext("left", Expression.class);
                final Optional<Expression> right = args.getNext("right", Expression.class);
                return builder.apply(left, right);
            };
        }

        private Function<AggregationArguments, Aggregation> function(
            final Function<Optional<Expression>, Aggregation> builder
        ) {
            return args -> {
                final Optional<Expression> reference = args.getNext("reference", Expression.class);
                return builder.apply(reference);
            };
        }

        private <T extends Aggregation> SamplingAggregationDSL<T> samplingBuilder(
            SamplingBuilder<T> builder
        ) {
            return new SamplingAggregationDSL<T>() {
                @Override
                protected T buildWith(
                    final AggregationArguments args, final Optional<Duration> size,
                    final Optional<Duration> extent, final Optional<Expression> reference
                ) {
                    return builder.apply(Optional.empty(), size, extent, reference);
                }
            };
        }

        interface SamplingBuilder<T> {
            T apply(
                Optional<SamplingQuery> sampling, Optional<Duration> size,
                Optional<Duration> extent, Optional<Expression> reference
            );
        }
    }
}
