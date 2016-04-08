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

package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationPipeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionDurationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionFloatContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionIntegerContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionListContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterAndContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterBooleanContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterHasContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterInContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterKeyEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterKeyNotEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotInContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotPrefixContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotRegexContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterOrContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterPrefixContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterRegexContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FromContext;
import com.spotify.heroic.grammar.HeroicQueryParser.KeyValueContext;
import com.spotify.heroic.grammar.HeroicQueryParser.QueryContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SelectAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeAbsoluteContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeRelativeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.StringContext;
import com.spotify.heroic.grammar.HeroicQueryParser.WhereContext;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
@RequiredArgsConstructor
public class QueryListener extends HeroicQueryBaseListener {
    private final FilterFactory filters;

    private static final Object LIST_MARK = new Object();
    private static final Object EXPR_FUNCTION_ENTER = new Object();

    public static final Object EMPTY = new Object();
    public static final Object NOT_EMPTY = new Object();

    public static final Object QUERY_MARK = new Object();
    public static final Object SELECT_MARK = new Object();
    public static final Object WHERE_MARK = new Object();
    public static final Object FROM_MARK = new Object();

    public static final Object PIPE_MARK = new Object();
    public static final Object STATEMENTS_MARK = new Object();

    public static final String GROUP = "group";
    public static final String CHAIN = "chain";

    private final Stack<Object> stack = new Stack<>();

    @Override
    public void enterStatements(final HeroicQueryParser.StatementsContext ctx) {
        push(STATEMENTS_MARK);
    }

    @Override
    public void exitStatements(final HeroicQueryParser.StatementsContext ctx) {
        final Context c = context(ctx);
        final List<Expression> expressions = popUntil(c, STATEMENTS_MARK, Expression.class);
        push(new Statements(c, expressions));
    }

    @Override
    public void exitLetStatement(final HeroicQueryParser.LetStatementContext ctx) {
        final Context c = context(ctx);
        final Expression e = pop(Expression.class);
        final ReferenceExpression reference = pop(ReferenceExpression.class);
        push(new LetExpression(c, reference, e));
    }

    @Override
    public void exitExpressionReference(
        final HeroicQueryParser.ExpressionReferenceContext ctx
    ) {
        final Context c = context(ctx);
        final String name = ctx.getChild(0).getText().substring(1);
        push(new ReferenceExpression(c, name));
    }

    @Override
    public void enterQuery(QueryContext ctx) {
        push(QUERY_MARK);
    }

    @Override
    public void exitQuery(QueryContext ctx) {
        final Context c = context(ctx);

        Optional<Expression> aggregation = Optional.empty();
        Optional<MetricType> source = Optional.empty();
        Optional<RangeExpression> range = Optional.empty();
        Optional<Filter> where = Optional.empty();

        while (true) {
            final Object mark = stack.pop();

            if (mark == QUERY_MARK) {
                break;
            }

            if (mark == SELECT_MARK) {
                aggregation = popOptional(Expression.class);
                continue;
            }

            if (mark == WHERE_MARK) {
                where = Optional.of(pop(Filter.class));
                continue;
            }

            if (mark == FROM_MARK) {
                source = Optional.of(pop(MetricType.class));
                range = popOptional(RangeExpression.class);
                continue;
            }

            throw c.error(String.format("expected part of query, but got %s", mark));
        }

        if (source == null) {
            throw c.error("No source clause available");
        }

        push(new QueryExpression(c, aggregation, source, range, where));
    }

    @Override
    public void exitExpressionNegate(
        final HeroicQueryParser.ExpressionNegateContext ctx
    ) {
        final Context c = context(ctx);
        final Expression expression = pop(c, Expression.class);
        push(new NegateExpression(c, expression));
    }

    @Override
    public void exitExpressionPlusMinus(
        final HeroicQueryParser.ExpressionPlusMinusContext ctx
    ) {
        final Context c = context(ctx);
        final Expression right = pop(c, Expression.class);
        final Expression left = pop(c, Expression.class);

        final String operator = ctx.getChild(1).getText();

        switch (operator) {
            case "+":
                push(new PlusExpression(c, left, right));
                break;
            case "-":
                push(new MinusExpression(c, left, right));
                break;
            default:
                throw c.error("Unsupported operator: " + operator);
        }
    }

    @Override
    public void exitExpressionDivMul(
        final HeroicQueryParser.ExpressionDivMulContext ctx
    ) {
        final Context c = context(ctx);
        final Expression right = pop(c, Expression.class);
        final Expression left = pop(c, Expression.class);

        final String operator = ctx.getChild(1).getText();

        switch (operator) {
            case "/":
                push(new DivideExpression(c, left, right));
                break;
            case "*":
                push(new MultiplyExpression(c, left, right));
                break;
            default:
                throw c.error("Unsupported operator: " + operator);
        }
    }

    @Override
    public void exitFilterIn(FilterInContext ctx) {
        final Context c = context(ctx);
        final Expression match = pop(c, Expression.class);
        final StringExpression key = pop(c, StringExpression.class);

        push(filters.or(buildIn(c, key, match)));
    }

    @Override
    public void exitFilterNotIn(FilterNotInContext ctx) {
        final Context c = context(ctx);
        final ListExpression match = pop(c, ListExpression.class);
        final StringExpression key = pop(c, StringExpression.class);

        push(filters.not(filters.or(buildIn(c, key, match))));
    }

    @Override
    public void exitSelectAll(final SelectAllContext ctx) {
        pushOptional(Optional.empty());
        push(SELECT_MARK);
    }

    @Override
    public void exitSelectExpression(final HeroicQueryParser.SelectExpressionContext ctx) {
        final Expression aggregation = pop(Expression.class);
        pushOptional(Optional.of(aggregation));
        push(SELECT_MARK);
    }

    @Override
    public void exitWhere(WhereContext ctx) {
        final Context c = context(ctx);
        push(pop(c, Filter.class));
        push(WHERE_MARK);
    }

    @Override
    public void enterExpressionList(ExpressionListContext ctx) {
        stack.push(LIST_MARK);
    }

    @Override
    public void exitExpressionList(ExpressionListContext ctx) {
        final Context c = context(ctx);
        stack.push(new ListExpression(c, popUntil(c, LIST_MARK, Expression.class)));
    }

    @Override
    public void enterExpressionFunction(HeroicQueryParser.ExpressionFunctionContext ctx) {
        stack.push(EXPR_FUNCTION_ENTER);
    }

    @Override
    public void exitExpressionFunction(final HeroicQueryParser.ExpressionFunctionContext ctx) {
        final Context c = context(ctx);

        final String name = ctx.getChild(0).getText();

        final ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        final ImmutableMap.Builder<String, Expression> keywords = ImmutableMap.builder();

        while (stack.peek() != EXPR_FUNCTION_ENTER) {
            final Object top = stack.pop();

            if (top instanceof KeywordValue) {
                final KeywordValue kw = (KeywordValue) top;
                keywords.put(kw.key, kw.expression);
                continue;
            }

            if (top instanceof Expression) {
                arguments.add((Expression) top);
                continue;
            }

            throw c.error(String.format("expected value, but got %s", top));
        }

        stack.pop();

        push(
            new FunctionExpression(c, name, new ListExpression(c, Lists.reverse(arguments.build())),
                keywords.build()));
    }

    @Override
    public void exitKeyValue(KeyValueContext ctx) {
        final Expression expression = pop(context(ctx), Expression.class);
        stack.push(new KeywordValue(ctx.getChild(0).getText(), expression));
    }

    @Override
    public void exitFrom(FromContext ctx) {
        final Context context = context(ctx);

        final String sourceText = ctx.getChild(1).getText();

        final MetricType source = MetricType
            .fromIdentifier(sourceText)
            .orElseThrow(() -> context.error("Invalid source (" + sourceText +
                "), must be one of " + MetricType.values()));

        final Optional<RangeExpression> range;

        if (ctx.getChildCount() > 2) {
            range = Optional.of(pop(context, RangeExpression.class));
        } else {
            range = Optional.empty();
        }

        pushOptional(range);
        push(source);
        push(FROM_MARK);
    }

    @Override
    public void exitSourceRangeAbsolute(SourceRangeAbsoluteContext ctx) {
        final Context c = context(ctx);
        final Expression end = pop(c, Expression.class);
        final Expression start = pop(c, Expression.class);
        push(new RangeExpression(c, start, end));
    }

    @Override
    public void exitSourceRangeRelative(SourceRangeRelativeContext ctx) {
        final Context c = context(ctx);
        final ReferenceExpression now = new ReferenceExpression(c, "now");
        final Expression distance = pop(c, Expression.class);
        final Expression start = new MinusExpression(c, now, distance);
        push(new RangeExpression(c, start, now));
    }

    @Override
    public void exitExpressionInteger(ExpressionIntegerContext ctx) {
        push(new IntegerExpression(context(ctx), Long.parseLong(ctx.getText())));
    }

    @Override
    public void exitExpressionFloat(ExpressionFloatContext ctx) {
        push(new DoubleExpression(context(ctx), Double.parseDouble(ctx.getText())));
    }

    @Override
    public void exitString(StringContext ctx) {
        final ParseTree child = ctx.getChild(0);
        final CommonToken token = (CommonToken) child.getPayload();
        final Context c = context(ctx);

        if (token.getType() == HeroicQueryLexer.SimpleString ||
            token.getType() == HeroicQueryLexer.Identifier) {
            push(new StringExpression(c, child.getText()));
            return;
        }

        push(new StringExpression(c, parseQuotedString(child.getText())));
    }

    @Override
    public void exitExpressionDuration(ExpressionDurationContext ctx) {
        final String text = ctx.getText();

        final int value;
        final TimeUnit unit;

        final Context c = context(ctx);

        if (text.length() > 2 && "ms".equals(text.substring(text.length() - 2, text.length()))) {
            unit = TimeUnit.MILLISECONDS;
            value = Integer.parseInt(text.substring(0, text.length() - 2));
        } else {
            unit = extractUnit(c, text.substring(text.length() - 1, text.length()));
            value = Integer.parseInt(text.substring(0, text.length() - 1));
        }

        push(new DurationExpression(c, unit, value));
    }

    @Override
    public void exitFilterHas(FilterHasContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(filters.hasTag(value.getString()));
    }

    @Override
    public void exitFilterNot(FilterNotContext ctx) {
        push(filters.not(pop(context(ctx), Filter.class)));
    }

    @Override
    public void exitFilterKeyEq(FilterKeyEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(filters.matchKey(value.getString()));
    }

    @Override
    public void exitFilterKeyNotEq(FilterKeyNotEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(filters.not(filters.matchKey(value.getString())));
    }

    @Override
    public void exitFilterEq(FilterEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.matchTag(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotEq(FilterNotEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.not(filters.matchTag(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterPrefix(FilterPrefixContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.startsWith(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotPrefix(FilterNotPrefixContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.not(filters.startsWith(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterRegex(FilterRegexContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.regex(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotRegex(FilterNotRegexContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(filters.not(filters.regex(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterAnd(FilterAndContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(filters.and(a, b));
    }

    @Override
    public void exitFilterOr(FilterOrContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(filters.or(a, b));
    }

    @Override
    public void exitAggregationBy(final AggregationByContext ctx) {
        final Context c = context(ctx);

        final ListExpression group = pop(c, Expression.class).cast(ListExpression.class);
        final FunctionExpression left = pop(c, Expression.class).cast(FunctionExpression.class);

        push(new FunctionExpression(c, GROUP, Expression.list(group, left), ImmutableMap.of()));
    }

    @Override
    public void exitAggregationByAll(final AggregationByAllContext ctx) {
        final Context c = context(ctx);

        final FunctionExpression left = pop(c, Expression.class).cast(FunctionExpression.class);

        push(new FunctionExpression(c, GROUP, Expression.list(new EmptyExpression(c), left),
            ImmutableMap.of()));
    }

    @Override
    public void enterAggregationPipe(AggregationPipeContext ctx) {
        stack.push(PIPE_MARK);
    }

    @Override
    public void exitAggregationPipe(AggregationPipeContext ctx) {
        final Context c = context(ctx);
        final List<FunctionExpression> values = ImmutableList.copyOf(
            popUntil(c, PIPE_MARK, Expression.class)
                .stream()
                .map(v -> v.cast(FunctionExpression.class))
                .iterator());
        push(new FunctionExpression(c, CHAIN, new ListExpression(c, values), ImmutableMap.of()));
    }

    @Override
    public void exitFilterBoolean(FilterBooleanContext ctx) {
        final Context c = context(ctx);
        final String literal = ctx.getText();

        if ("true".equals(literal)) {
            push(filters.t());
            return;
        }

        if ("false".equals(literal)) {
            push(filters.f());
            return;
        }

        throw c.error("unsupported boolean literal: " + literal);
    }

    private <T> List<T> popUntil(final Context c, final Object mark, final Class<T> type) {
        final ImmutableList.Builder<T> results = ImmutableList.builder();

        while (stack.peek() != mark) {
            results.add(pop(c, type));
        }

        stack.pop();
        return Lists.reverse(results.build());
    }

    private List<Filter> buildIn(
        final Context c, final StringExpression key, final Expression match
    ) {
        if (match instanceof StringExpression) {
            return ImmutableList.of(filters.matchTag(key.getString(), match.cast(String.class)));
        }

        if (!(match instanceof ListExpression)) {
            throw c.error("Cannot use type " + match + " in expression");
        }

        final List<Filter> values = new ArrayList<>();

        for (final Expression v : ((ListExpression) match).getList()) {
            values.add(filters.matchTag(key.getString(), v.cast(String.class)));
        }

        return values;
    }

    private TimeUnit extractUnit(Context ctx, String text) {
        if ("s".equals(text)) {
            return TimeUnit.SECONDS;
        }

        if ("m".equals(text)) {
            return TimeUnit.MINUTES;
        }

        if ("H".equals(text) || "h".equals(text)) {
            return TimeUnit.HOURS;
        }

        if ("d".equals(text)) {
            return TimeUnit.DAYS;
        }

        throw ctx.error("illegal unit: " + text);
    }

    private String parseQuotedString(String text) {
        int i = 0;
        boolean escapeNext = false;

        final StringBuilder builder = new StringBuilder();

        while (i < text.length()) {
            final char c = text.charAt(i++);

            if (i == 1 || i == text.length()) {
                continue;
            }

            if (escapeNext) {
                if (c == 'b') {
                    builder.append("\b");
                } else if (c == 't') {
                    builder.append("\t");
                } else if (c == 'n') {
                    builder.append("\n");
                } else if (c == 'f') {
                    builder.append("\f");
                } else if (c == 'r') {
                    builder.append("\r");
                } else {
                    builder.append(c);
                }

                escapeNext = false;
                continue;
            }

            if (c == '\\') {
                escapeNext = true;
                continue;
            }

            builder.append(c);
        }

        return builder.toString();
    }

    public void popMark(Object mark) {
        final Object actual = pop(Object.class);

        if (actual != mark) {
            throw new IllegalStateException("Expected mark " + mark + ", but got " + actual);
        }
    }

    public <T> List<T> popList(Class<T> type) {
        final Class<?> typeOn = pop(Class.class);
        checkType(type, typeOn);
        return (List<T>) pop(List.class);
    }

    public <T> Optional<T> popOptional(Class<T> type) {
        final Object mark = stack.pop();

        if (mark == EMPTY) {
            return Optional.empty();
        }

        if (mark == NOT_EMPTY) {
            return Optional.of(pop(type));
        }

        throw new IllegalStateException("stack does not contain a legal optional mark");
    }

    public <T> T pop(Class<T> type) {
        if (stack.isEmpty()) {
            throw new IllegalStateException("stack is empty (did you parse something?)");
        }

        final Object popped = stack.pop();

        checkType(type, popped.getClass());

        return (T) popped;
    }

    private <T> void checkType(Class<T> expected, Class<?> actual) {
        if (!expected.isAssignableFrom(actual)) {
            throw new IllegalStateException(
                String.format("expected %s, but was %s", name(expected), name(actual)));
        }
    }

    /* internals */
    private <T> void pushOptional(final Optional<T> value) {
        if (!value.isPresent()) {
            push(EMPTY);
            return;
        }

        push(value.get());
        push(NOT_EMPTY);
    }

    private void push(Object value) {
        stack.push(Objects.requireNonNull(value));
    }

    private <T> T pop(Context ctx, Class<T> type) {
        if (stack.isEmpty()) {
            throw ctx.error(String.format("expected %s, but was empty", name(type)));
        }

        final Object popped = stack.pop();
        checkType(type, popped.getClass());
        return (T) popped;
    }

    @Data
    private static final class KeywordValue {
        private final String key;
        private final Expression expression;
    }

    private static Context context(final ParserRuleContext source) {
        int line = source.getStart().getLine();
        int col = source.getStart().getCharPositionInLine();
        int lineEnd = source.getStop().getLine();
        int colEnd = source.getStop().getCharPositionInLine();
        return new Context(line, col, lineEnd, colEnd);
    }

    private static String name(Class<?> type) {
        final ValueName name = type.getAnnotation(ValueName.class);

        if (name == null) {
            return type.getName();
        }

        return "<" + name.value() + ">";
    }
}
