package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.CoreQueryParser.FromDSL;
import com.spotify.heroic.metric.MetricType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.Expression.duration;
import static com.spotify.heroic.grammar.Expression.integer;
import static com.spotify.heroic.grammar.Expression.let;
import static com.spotify.heroic.grammar.Expression.list;
import static com.spotify.heroic.grammar.Expression.minus;
import static com.spotify.heroic.grammar.Expression.plus;
import static com.spotify.heroic.grammar.Expression.query;
import static com.spotify.heroic.grammar.Expression.range;
import static com.spotify.heroic.grammar.Expression.reference;
import static com.spotify.heroic.grammar.Expression.string;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;

@RunWith(MockitoJUnitRunner.class)
public class QueryParserTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private CoreQueryParser parser;
    private FilterFactory filters;
    private AggregationFactory aggregations;

    @Mock
    Filter.MatchTag matchTag;
    @Mock
    Filter.And and;
    @Mock
    Filter.Or or;
    @Mock
    Filter optimized;

    Expression.Scope scope;

    @Before
    public void setupFilters() {
        filters = Mockito.mock(FilterFactory.class);
        aggregations = Mockito.mock(AggregationFactory.class);

        Mockito
            .when(filters.matchTag(Mockito.any(String.class), Mockito.any(String.class)))
            .thenReturn(matchTag);
        Mockito.when(filters.and(anyFilter(), anyFilter())).thenReturn(and);
        Mockito.when(filters.or(anyFilter(), anyFilter())).thenReturn(or);
        Mockito.when(filters.and((List<Filter>) anyList())).thenReturn(and);
        Mockito.when(filters.or((List<Filter>) anyList())).thenReturn(or);
        Mockito.when(or.optimize()).thenReturn(optimized);
        Mockito.when(and.optimize()).thenReturn(optimized);

        parser = new CoreQueryParser(filters);

        scope = new DefaultScope(10000);
    }

    @Test
    public void testList() {
        assertEquals(
            Expression.list(Expression.number(1), Expression.number(2), Expression.number(3)),
            expr("[1, 2, 3]"));
        assertEquals(expr("[1, 2, 3]"), expr("{1, 2, 3}"));
    }

    @Test
    public void testAggregation() {
        final Expression d = duration(TimeUnit.HOURS, 30);

        assertEquals(a("average", d), aggregation("average(30H)"));
        assertEquals(a("sum", d), aggregation("sum(30H)"));

        final FunctionExpression chain =
            a("chain", a("group", Expression.list(Expression.string("host")), a("average", d)),
                a("sum", d));

        assertEquals(chain, aggregation("chain(group([host], average(30H)), sum(30H))"));
        assertEquals(chain, aggregation("average(30H) by host | sum(30H)"));

        assertEquals(a("chain", a("group", list(string("host")), a("average")),
            a("group", list(string("site")), a("sum"))),
            aggregation("average by host | sum by site"));

        // test grouping
        assertEquals(a("group", list(string("site")),
            a("chain", a("group", list(string("host")), a("average")), a("sum"))),
            aggregation("(average by host | sum) by site"));
    }

    @Test
    public void testByAll() {
        final FunctionExpression reference = Expression.aggregation("group",
            Expression.list(Expression.empty(), Expression.aggregation("average")));
        assertEquals(reference, parser.parse(CoreQueryParser.AGGREGATION, "average by *"));
    }

    @Test
    public void testArithmetics() {
        final Expression foo = expr("foo"), bar = expr("bar");

        // numbers
        assertEquals((Long) 3L, expr("1 + 2 + 3 - 3").eval(scope).cast(Long.class));

        // two strings
        assertEquals("foobar", expr("foo + bar").eval(scope).cast(String.class));

        // two lists
        assertEquals(Expression.list(foo, bar),
            expr("[foo] + [bar]").eval(scope).cast(ListExpression.class));

        // durations
        assertEquals(duration(TimeUnit.MINUTES, 55), expr("1H - 5m").eval(scope));
        assertEquals(duration(TimeUnit.HOURS, 7), expr("3H + 4H").eval(scope));
        assertEquals(duration(TimeUnit.MINUTES, 59), expr("119m - 1H").eval(scope));
        assertEquals(duration(TimeUnit.MINUTES, 60 * 11), expr("1H + 1m - 1m + 10H").eval(scope));
    }

    @Test
    public void testFrom() {
        checkFrom(MetricType.POINT, empty(), from("from points"));
        checkFrom(MetricType.EVENT, empty(), from("from events"));

        final Optional<Expression> r1 = of(range(integer(0), integer(1000)));

        // absolute
        checkFrom(MetricType.POINT, r1, from("from points(0, 400 + 600)").eval(scope));

        final Optional<Expression> r2 = of(range(integer(9000), integer(10000)));

        // relative
        checkFrom(MetricType.POINT, r2, from("from points(1000ms)").eval(scope));
    }

    @Test
    public void testFilter1() {
        assertEquals(optimized,
            parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in {foo, bar}"));

        Mockito
            .verify(filters, Mockito.times(4))
            .matchTag(Mockito.any(String.class), Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    @Test
    public void testFilter2() {
        assertEquals(optimized,
            parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in [foo, bar]"));

        Mockito
            .verify(filters, Mockito.times(4))
            .matchTag(Mockito.any(String.class), Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    @Test
    public void testUnterminatedString() {
        exception.expect(ParseException.class);
        exception.expectMessage("unterminated string");
        parser.parse(CoreQueryParser.EXPRESSION, "\"open");
    }

    @Test
    public void testInvalidSelect() {
        exception.expect(ParseException.class);
        parser.parse(CoreQueryParser.EXPRESSION, "%1");
    }

    @Test
    public void testInvalidGrammar() {
        exception.expect(ParseException.class);
        exception.expectMessage("unexpected token: ~");
        parser.parse(CoreQueryParser.QUERY, "~ from points");
    }

    @Test
    public void testMultipleStatements() {
        final StringBuilder query = new StringBuilder();
        query.append("let $a = * from points(1d);\n");
        query.append("let $b = * from points($now - 2d, $now - 1d);\n");
        query.append("$a + $b");

        final Statements statements = parser.parse(CoreQueryParser.STATEMENTS, query.toString());

        // @formatter:off
        final List<Expression> expected = ImmutableList.of(
            let(reference("a"),
                query(empty(), of(MetricType.POINT),
                    of(range(
                        minus(reference("now"),
                            duration(TimeUnit.DAYS, 1)),
                        reference("now"))), empty(), ImmutableMap.of())),
            let(reference("b"),
                query(empty(), of(MetricType.POINT),
                    of(range(
                        minus(reference("now"), duration(TimeUnit.DAYS, 2)),
                        minus(reference("now"), duration(TimeUnit.DAYS, 1)))),
                    empty(), ImmutableMap.of())),
            query(of(plus(reference("a"), reference("b"))), empty(), empty(), empty(),
                ImmutableMap.of()));
        // @formatter:on

        assertEquals(expected, statements.getExpressions());
    }

    public static FunctionExpression a(final String name, final Expression... expressions) {
        return Expression.aggregation(name,
            new ListExpression(Context.empty(), ImmutableList.copyOf(expressions)));
    }

    void checkFrom(
        MetricType source, Optional<Expression> range, CoreQueryParser.FromDSL result
    ) {
        assertEquals(source, result.getSource());
        assertEquals(range, result.getRange());
    }

    private FunctionExpression aggregation(String input) {
        return parser.parse(CoreQueryParser.AGGREGATION, input);
    }

    static Filter anyFilter() {
        return any(Filter.class);
    }

    Expression expr(String input) {
        return parser.parse(CoreQueryParser.EXPRESSION, input);
    }

    FromDSL from(String input) {
        return parser.parse(CoreQueryParser.FROM, input);
    }
}
