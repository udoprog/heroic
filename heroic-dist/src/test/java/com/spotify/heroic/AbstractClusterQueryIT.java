package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.ingestion.WriteOptions;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ResultLimit;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Tracing;
import eu.toolchain.async.AsyncFuture;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.spotify.heroic.test.Data.points;
import static com.spotify.heroic.test.Matchers.containsChild;
import static com.spotify.heroic.test.Matchers.hasIdentifier;
import static com.spotify.heroic.test.Matchers.identifierContains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public abstract class AbstractClusterQueryIT extends AbstractLocalClusterIT {
    private final Series s1 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "a"));
    private final Series s2 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "b"));
    private final Series s3 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "c"));

    private QueryManager query;

    protected boolean cardinalitySupport = true;

    protected void setupSupport() {
    }

    @Before
    public final void setupAbstract() {
        setupSupport();

        query = instances.get(0).inject(CoreComponent::queryManager);
    }

    @Override
    protected AsyncFuture<Void> prepareEnvironment() {
        final List<IngestionManager> ingestion = instances
            .stream()
            .map(i -> i.inject(IngestionComponent::ingestionManager))
            .collect(Collectors.toList());

        final List<AsyncFuture<Ingestion>> writes = new ArrayList<>();

        final IngestionManager m1 = ingestion.get(0);
        final IngestionManager m2 = ingestion.get(1);

        writes.add(m1
            .useDefaultGroup()
            .write(new Ingestion.Request(WriteOptions.defaults(), s1,
                points().p(10, 1D).p(30, 2D).build())));
        writes.add(m2
            .useDefaultGroup()
            .write(new Ingestion.Request(WriteOptions.defaults(), s2,
                points().p(10, 1D).p(20, 4D).build())));

        return async.collectAndDiscard(writes);
    }

    public QueryResult query(final String queryString) throws Exception {
        return query(query.newQueryFromString(queryString), (builder, options) -> {
        });
    }

    public QueryResult query(
        final String queryString, final BiConsumer<QueryBuilder, QueryOptions.Builder> modifier
    ) throws Exception {
        return query(query.newQueryFromString(queryString), modifier);
    }

    public QueryResult query(
        final QueryBuilder builder, final BiConsumer<QueryBuilder, QueryOptions.Builder> modifier
    ) throws Exception {
        final QueryOptions.Builder options = QueryOptions.builder();
        options.tracing(Tracing.enabled());

        builder
            .features(Optional.of(FeatureSet.of(Feature.DISTRIBUTED_AGGREGATIONS)))
            .source(Optional.of(MetricType.POINT))
            .rangeIfAbsent(Optional.of(new QueryDateRange.Absolute(10, 40)));

        modifier.accept(builder, options);

        builder.options(Optional.of(options.build()));

        final Query finalQuery = builder.build();
        final QueryResult result = query.useDefaultGroup().query(finalQuery).get();

        if (finalQuery.getOptions().orElseGet(QueryOptions::defaults).getTracing().isEnabled()) {
            /* verify that traces works, if enabled */
            for (final URI uri : instanceUris()) {
                // TODO: grpc URIs are configured with localhost:<port>.
                if (!"grpc".equals(uri.getScheme())) {
                    assertThat(result.getTrace(),
                        containsChild(hasIdentifier(identifierContains(uri.toString()))));
                }
            }
        }

        return result;
    }

    @Test
    public void basicQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 4D).p(30, 2D).build()), m);
    }

    @Test
    public void distributedQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by shared");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 4D).p(30, 2D).build()), m);
    }

    @Test
    public void distributedQueryTraceTest() throws Exception {
        final QueryResult result = query("sum(10ms) by shared", (builder, options) -> {
        });

        // Verify that the top level QueryTrace is for CoreQueryManager
        assertThat(result.getTrace(), hasIdentifier(equalTo(CoreQueryManager.QUERY)));
        /* Verify that the first level of QueryTrace children contains at least one entry for the
         * local node and at least one for the remote node */
        assertThat(result.getTrace(), containsChild(hasIdentifier(identifierContains("[local]"))));
        assertThat(result.getTrace(),
            containsChild(hasIdentifier(not(identifierContains("[local]")))));
    }

    @Test
    public void distributedDifferentQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by diff");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L, 10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(30, 2D).build(),
            points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void distributedFilterQueryTest() throws Exception {
        final QueryResult result = query("average(10ms) by * | topk(2) | bottomk(1) | sum(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void filterQueryTest() throws Exception {
        final QueryResult result =
            query("average(10ms) by * | topk(2) | bottomk(1) | sum(10ms)", (builder, options) -> {
                builder.features(Optional.empty());
            });

        final PrintWriter writer = new PrintWriter(System.out);
        result.getTrace().formatTrace(writer);
        writer.flush();

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        // why not two responses?
        assertEquals(ImmutableList.of(10L, 10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 4D).build(),
            points().p(10, 1D).p(30, 2D).build()), m);
    }

    @Test
    public void deltaQueryTest() throws Exception {
        final QueryResult result = query("delta", (builder, options) -> {
            builder.features(Optional.empty());
        });

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(-1L, -1L), cadences);
        assertEquals(ImmutableSet.of(points().p(30, 1D).build(), points().p(20, 3D).build()), m);
    }

    @Test
    public void distributedDeltaQueryTest() throws Exception {
        final QueryResult result = query("max | delta");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(1L), cadences);
        assertEquals(ImmutableSet.of(points().p(20, 3D).p(30, -2D).build()), m);
    }

    @Test
    public void filterLastQueryTest() throws Exception {
        final QueryResult result = query("average(10ms) by * | topk(2) | bottomk(1)");

        for (final RequestError error : result.getErrors()) {
            System.out.println(error);
        }

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void cardinalityTest() throws Exception {
        assumeTrue(cardinalitySupport);

        final QueryResult result = query("cardinality(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 1D).p(30, 1D).p(40, 0D).build()), m);
    }

    @Test
    public void cardinalityWithKeyTest() throws Exception {
        assumeTrue(cardinalitySupport);

        // TODO: support native booleans in expressions
        final QueryResult result = query("cardinality(10ms, method=hllp(includeKey=\"true\"))");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 1D).p(30, 1D).p(40, 0D).build()), m);
    }

    @Test
    public void dataLimit() throws Exception {
        final QueryResult result = query("*", (builder, options) -> {
            options.dataLimit(1L);
        });

        // quota limits are always errors
        assertThat(result.getErrors(), iterableWithSize(2));

        for (final RequestError e : result.getErrors()) {
            assertTrue((e instanceof QueryError));
            final QueryError q = (QueryError) e;
            assertThat(q.getError(),
                containsString("Some fetches failed (1) or were cancelled (0)"));
        }

        assertEquals(ResultLimits.of(ResultLimit.QUOTA), result.getLimits());
    }

    @Test
    public void groupLimit() throws Exception {
        final QueryResult result = query("*", (builder, options) -> {
            options.groupLimit(1L);
        });

        assertEquals(0, result.getErrors().size());
        assertEquals(ResultLimits.of(ResultLimit.GROUP), result.getLimits());
        assertEquals(1, result.getGroups().size());
    }

    @Test
    public void seriesLimitFailure() throws Exception {
        final QueryResult result = query("*", (builder, options) -> {
            options.seriesLimit(0L).failOnLimits(true);
        });

        assertEquals(2, result.getErrors().size());

        for (final RequestError e : result.getErrors()) {
            assertTrue((e instanceof QueryError));
            final QueryError q = (QueryError) e;
            assertThat(q.getError(), containsString(
                "The number of series requested is more than the allowed limit of [0]"));
        }

        assertEquals(ResultLimits.of(ResultLimit.SERIES), result.getLimits());
    }

    @Test
    public void groupLimitFailure() throws Exception {
        final QueryResult result = query("*", (builder, options) -> {
            options.groupLimit(0L).failOnLimits(true);
        });

        assertEquals(2, result.getErrors().size());

        for (final RequestError e : result.getErrors()) {
            assertTrue((e instanceof QueryError));
            final QueryError q = (QueryError) e;
            assertThat(q.getError(), containsString(
                "The number of result groups is more than the allowed limit of [0]"));
        }

        assertEquals(ResultLimits.of(ResultLimit.GROUP), result.getLimits());
        assertEquals(0, result.getGroups().size());
    }

    private Set<MetricCollection> getResults(final QueryResult result) {
        return result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getMetrics)
            .collect(Collectors.toSet());
    }

    private List<Long> getCadences(final QueryResult result) {
        final List<Long> cadences = result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getCadence)
            .collect(Collectors.toList());

        Collections.sort(cadences);
        return cadences;
    }
}
