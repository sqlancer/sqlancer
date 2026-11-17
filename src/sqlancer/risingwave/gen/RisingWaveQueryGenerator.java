package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveAggregateFunction;
import sqlancer.risingwave.ast.RisingWaveAlias;
import sqlancer.risingwave.ast.RisingWaveColumnReference;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveFunctionCall;
import sqlancer.risingwave.ast.RisingWaveJoin;
import sqlancer.risingwave.ast.RisingWaveOperation.Operator;
import sqlancer.risingwave.ast.RisingWaveRelation;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.ast.RisingWaveSubquery;

/** Coordinates outer TLP blocks and independently scoped query blocks. */
public final class RisingWaveQueryGenerator implements
        TLPWhereGenerator<RisingWaveSelect, RisingWaveJoin, RisingWaveExpression, RisingWaveTable, RisingWaveColumn>,
        ExpressionGenerator<RisingWaveExpression> {
    private final RisingWaveGlobalState state;
    private RisingWaveGenerationContext context;
    private RisingWaveExpressionGenerator expressions;
    private RisingWaveRelation root;

    public RisingWaveQueryGenerator(RisingWaveGlobalState state) {
        this.state = state;
        state.getDbmsSpecificOptions().validate();
    }

    @Override
    public RisingWaveQueryGenerator setTablesAndColumns(AbstractTables<RisingWaveTable, RisingWaveColumn> tables) {
        context = new RisingWaveGenerationContext(state, tables.getTables());
        expressions = new RisingWaveExpressionGenerator(context);
        root = new RisingWaveRelationGenerator(context, expressions).generate(context.options.relationDepth);
        return this;
    }

    static RisingWaveSelect select(RisingWaveRelation source) {
        RisingWaveSelect query = new RisingWaveSelect();
        query.setFromList(List.of(source));
        return query;
    }

    static RisingWaveSelect block(RisingWaveGenerationContext context, RisingWaveExpressionGenerator expressions,
            RisingWaveRelation source, boolean grouped) {
        RisingWaveSelect query = select(source);
        List<RisingWaveExpression> outputs = new ArrayList<>();
        if (grouped) {
            // Floating-point grouping can expose different signed-zero representations.
            List<RisingWaveColumnReference> eligible = source.columns().stream()
                    .filter(c -> !c.type.floatingPoint() && !c.type.struct() && !c.type.array())
                    .collect(java.util.stream.Collectors.toList());
            List<RisingWaveColumnReference> keys = Randomly.getBooleanWithRatherLowProbability() || eligible.isEmpty()
                    ? Collections.emptyList() : Randomly.nonEmptySubset(eligible);
            // Keep grouping keys and sets small to limit query size.
            keys = keys.subList(0, Math.min(3, keys.size()));
            query.setGroupByClause(new ArrayList<>(keys));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                List<List<RisingWaveExpression>> sets = new ArrayList<>();
                int number = (int) Randomly.getNotCachedInteger(2, 4);
                for (int i = 0; i < number; i++) {
                    sets.add(keys.isEmpty() || Randomly.getBoolean() ? List.of()
                            : new ArrayList<>(Randomly.nonEmptySubset(keys)));
                }
                query.setGroupingSets(sets);
                // Only columns present in at least one grouping set may be projected or used in HAVING.
                keys = keys.stream().filter(k -> sets.stream().anyMatch(s -> s.contains(k)))
                        .collect(java.util.stream.Collectors.toList());
                query.setGroupByClause(new ArrayList<>(keys));
            }
            List<RisingWaveExpression> havingInputs = new ArrayList<>(keys);
            for (RisingWaveColumnReference key : keys) {
                outputs.add(new RisingWaveAlias(key, "c" + outputs.size()));
            }
            int count = (int) Randomly.getNotCachedInteger(1, 4);
            for (int i = 0; i < count; i++) {
                RisingWaveColumnReference column = Randomly.fromList(source.columns());
                RisingWaveFunctionCall aggregate = Randomly.getBoolean() ? expressions.count(column)
                        : expressions.aggregate(column);
                aggregate = expressions.modifiers(aggregate, source.columns());
                outputs.add(new RisingWaveAlias(aggregate, "c" + outputs.size()));
                havingInputs.add(aggregate);
            }
            if (Randomly.getBoolean()) {
                query.setHavingClause(havingPredicate(expressions, havingInputs));
            }
        } else {
            List<RisingWaveColumnReference> columns = Randomly.nonEmptySubset(source.columns());
            for (int i = 0; i < Math.min(5, columns.size()); i++) {
                RisingWaveExpression expression = expressions.expression(
                        RisingWaveExpressionGenerator.outputType(columns.get(i)), source.columns(), 0, true);
                outputs.add(new RisingWaveAlias(expression, "c" + i));
            }
        }
        query.setFetchColumns(outputs);
        if (Randomly.getBooleanWithSmallProbability()) {
            query.setWhereClause(new RisingWaveConstant(RisingWaveType.BOOLEAN, "false"));
        } else if (Randomly.getBoolean()) {
            // Generate IN only as a direct filter, avoiding the outer TLP IS NULL wrapper.
            RisingWaveType type = Randomly.fromList(source.columns()).type;
            if (context.streaming && context.correlatedExistsBudget > 0
                    && Randomly.fromOptions(false, false, false, true)) {
                query.setWhereClause(expressions.existsFilter(type, source.columns()));
            } else if (!context.streaming && Randomly.getBoolean()) {
                query.setWhereClause(Randomly.getBoolean()
                        ? expressions.subquery(type, RisingWaveSubquery.Kind.IN,
                                expressions.leaf(type, source.columns()))
                        : expressions.existsFilter(type, source.columns()));
            } else {
                query.setWhereClause(expressions.expression(RisingWaveType.BOOLEAN, source.columns(), 0, true));
            }
        }
        return query;
    }

    private static RisingWaveExpression havingPredicate(RisingWaveExpressionGenerator expressions,
            List<RisingWaveExpression> inputs) {
        RisingWaveExpression having = null;
        for (int i = 0; i < (int) Randomly.getNotCachedInteger(1, 4); i++) {
            RisingWaveExpression argument = Randomly.fromList(inputs);
            RisingWaveExpression term = Randomly.getBooleanWithRatherLowProbability() ? expressions.isNull(argument)
                    : expressions.havingComparison(argument);
            having = having == null ? term : expressions.binary(Randomly.fromOptions(Operator.AND, Operator.OR),
                    RisingWaveType.BOOLEAN, having, term);
        }
        return having;
    }

    public RisingWaveSelect generateAggregateSelect() {
        RisingWaveAggregateFunction function = RisingWaveAggregateFunction.randomForTlp();
        List<RisingWaveColumnReference> eligible = root.columns().stream().filter(c -> function.acceptsTlpInput(c.type))
                .collect(java.util.stream.Collectors.toList());
        RisingWaveType type = function.requiresBooleanInput() ? RisingWaveType.BOOLEAN
                : eligible.isEmpty() ? RisingWaveType.INT : Randomly.fromList(eligible).type;
        List<RisingWaveExpression> arguments = function.allowsStar() && Randomly.getBoolean() ? List.of()
                : List.of(expressions.expression(type, root.columns(), 0, false));
        RisingWaveSelect query = select(root);
        query.setFetchColumns(List.of(new RisingWaveFunctionCall(function, arguments)));
        if (Randomly.getBoolean()) {
            query.setWhereClause(expressions.expression(RisingWaveType.BOOLEAN, root.columns(), 0, true));
        }
        return query;
    }

    public RisingWaveSelect generateGroupedSelect() {
        RisingWaveSelect query = block(context, expressions, root, true);
        query.setHavingClause(null);
        return query;
    }

    public RisingWaveExpression generateHavingPredicate(RisingWaveSelect query) {
        // Unwrap output names: HAVING binds to grouping expressions and aggregates, not SELECT aliases.
        List<RisingWaveExpression> inputs = new ArrayList<>();
        for (RisingWaveExpression output : query.getFetchColumns()) {
            inputs.add(((RisingWaveAlias) output).expression);
        }
        return havingPredicate(expressions, inputs);
    }

    @Override
    public RisingWaveExpression generatePredicate() {
        return generateBooleanExpression();
    }

    @Override
    public RisingWaveExpression generateBooleanExpression() {
        return expressions.expression(RisingWaveType.BOOLEAN, root.columns(), 0, true);
    }

    @Override
    public RisingWaveExpression negatePredicate(RisingWaveExpression predicate) {
        return expressions.negatePredicate(predicate);
    }

    @Override
    public RisingWaveExpression isNull(RisingWaveExpression expression) {
        return expressions.isNull(expression);
    }

    @Override
    public RisingWaveSelect generateSelect() {
        return new RisingWaveSelect();
    }

    @Override
    public List<RisingWaveJoin> getRandomJoinClauses() {
        return Collections.emptyList();
    }

    @Override
    public List<RisingWaveExpression> getTableRefs() {
        return List.of(root);
    }

    @Override
    public List<RisingWaveExpression> generateFetchColumns(boolean shouldCreateDummy) {
        List<RisingWaveColumnReference> eligible = root.columns().stream()
                .filter(c -> !c.type.floatingPoint() && !c.type.struct()).collect(java.util.stream.Collectors.toList());
        if (eligible.isEmpty()) {
            return List.of(expressions.isNull(Randomly.fromList(root.columns())));
        }
        List<RisingWaveExpression> columns = new ArrayList<>(Randomly.nonEmptySubset(eligible));
        return columns.subList(0, Math.min(3, columns.size()));
    }

    @Override
    public List<RisingWaveExpression> generateOrderBys() {
        List<RisingWaveColumnReference> eligible = root.columns().stream()
                .filter(c -> !c.type.array() && !c.type.struct()).collect(java.util.stream.Collectors.toList());
        return eligible.isEmpty() ? List.of() : List.of(Randomly.fromList(eligible));
    }
}
