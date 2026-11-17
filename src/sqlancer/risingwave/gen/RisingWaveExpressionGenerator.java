package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveAggregateFunction;
import sqlancer.risingwave.ast.RisingWaveAggregateOrder;
import sqlancer.risingwave.ast.RisingWaveArrayAccess;
import sqlancer.risingwave.ast.RisingWaveCase;
import sqlancer.risingwave.ast.RisingWaveCast;
import sqlancer.risingwave.ast.RisingWaveColumnReference;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveFieldAccess;
import sqlancer.risingwave.ast.RisingWaveFunctionCall;
import sqlancer.risingwave.ast.RisingWaveOperation;
import sqlancer.risingwave.ast.RisingWaveOperation.Operator;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.ast.RisingWaveSubquery;
import sqlancer.risingwave.ast.RisingWaveTableReference;

public class RisingWaveExpressionGenerator {
    /** Supported deterministic scalar overloads, selected by the required result type. */
    private enum RisingWaveScalarFunction {
        CONCAT("concat", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT),
        CONCAT_WS("concat_ws", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT),
        STRPOS("strpos", RisingWaveType.INT, RisingWaveType.TEXT, RisingWaveType.TEXT),
        TRANSLATE("translate", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT),
        LOWER("lower", RisingWaveType.TEXT, RisingWaveType.TEXT),
        UPPER("upper", RisingWaveType.TEXT, RisingWaveType.TEXT),
        TRIM("trim", RisingWaveType.TEXT, RisingWaveType.TEXT),
        LENGTH("length", RisingWaveType.INT, RisingWaveType.TEXT),
        ABS_INT("abs", RisingWaveType.INT, RisingWaveType.INT),
        ABS_SMALLINT("abs", RisingWaveType.SMALLINT, RisingWaveType.SMALLINT),
        ABS_REAL("abs", RisingWaveType.REAL, RisingWaveType.REAL),
        ABS_DOUBLE("abs", RisingWaveType.DOUBLE, RisingWaveType.DOUBLE),
        ABS_BIGINT("abs", RisingWaveType.BIGINT, RisingWaveType.BIGINT),
        ABS_DECIMAL("abs", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL),
        ROUND("round", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL),
        FLOOR("floor", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL),
        CEIL("ceil", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL),
        REPLACE("replace", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.TEXT),
        LTRIM("ltrim", RisingWaveType.TEXT, RisingWaveType.TEXT),
        RTRIM("rtrim", RisingWaveType.TEXT, RisingWaveType.TEXT),
        REVERSE("reverse", RisingWaveType.TEXT, RisingWaveType.TEXT),
        OCTET_LENGTH("octet_length", RisingWaveType.INT, RisingWaveType.TEXT),
        LEFT("left", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.INT),
        RIGHT("right", RisingWaveType.TEXT, RisingWaveType.TEXT, RisingWaveType.INT),
        INITCAP("initcap", RisingWaveType.TEXT, RisingWaveType.TEXT),
        SIGN("sign", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL),
        TRUNC("trunc", RisingWaveType.DECIMAL, RisingWaveType.DECIMAL);

        private final String sql;
        private final RisingWaveType result;
        private final List<RisingWaveType> arguments;

        RisingWaveScalarFunction(String sql, RisingWaveType result, RisingWaveType... arguments) {
            this.sql = sql;
            this.result = result;
            this.arguments = List.of(arguments);
        }

        static List<RisingWaveScalarFunction> returning(RisingWaveType type) {
            return Arrays.stream(values()).filter(f -> f.result == type).collect(Collectors.toList());
        }
    }

    private enum ExpressionKind {
        CASE, COALESCE, SCALAR_FUNCTION, COMPARISON_OR_ARITHMETIC, LOGICAL_OR_CAST, UNARY_OR_CAST, SCALAR_SUBQUERY,
        ARRAY_ACCESS, BOOLEAN_PREDICATE
    }

    private enum PredicateKind {
        LIKE, BETWEEN, IN
    }

    private final RisingWaveGenerationContext context;

    RisingWaveExpressionGenerator(RisingWaveGenerationContext context) {
        this.context = context;
    }

    RisingWaveFunctionCall count(RisingWaveExpression argument) {
        RisingWaveExpression input = argument.getType().struct() ? new RisingWaveFieldAccess(argument, 0) : argument;
        return new RisingWaveFunctionCall(RisingWaveAggregateFunction.COUNT, List.of(input));
    }

    RisingWaveFunctionCall modifiers(RisingWaveFunctionCall call, List<RisingWaveColumnReference> scope) {
        boolean arrayInput = call.arguments.stream().anyMatch(a -> a.getType().array());
        boolean distinct = !arrayInput && Randomly.getBooleanWithRatherLowProbability();
        List<RisingWaveExpression> arguments = call.aggregate.allowsStar() && !distinct
                && Randomly.getBooleanWithRatherLowProbability() ? List.of() : call.arguments;
        RisingWaveExpression filter = Randomly.getBoolean() ? expression(RisingWaveType.BOOLEAN, scope, 0, false)
                : null;
        List<RisingWaveAggregateOrder> order = arguments.isEmpty() || arrayInput ? List.of()
                : call.aggregate.requiresOrdering() || Randomly.getBooleanWithRatherLowProbability()
                        ? List.of(order(arguments.get(0))) : List.of();
        return new RisingWaveFunctionCall(call.aggregate, arguments, distinct, filter, order);
    }

    private RisingWaveAggregateOrder order(RisingWaveExpression argument) {
        return new RisingWaveAggregateOrder(argument, Randomly.fromOptions(RisingWaveAggregateOrder.Direction.values()),
                Randomly.fromOptions(RisingWaveAggregateOrder.NullOrder.values()));
    }

    RisingWaveFunctionCall aggregate(RisingWaveColumnReference column) {
        if (column.type.struct()) {
            return aggregateArgument(new RisingWaveFieldAccess(column, 0));
        }
        if (column.type.array()) {
            return Randomly.getBoolean() ? count(column) : aggregateArgument(
                    new RisingWaveArrayAccess(column, new RisingWaveConstant(RisingWaveType.INT, "1")));
        }
        RisingWaveExpression argument = Randomly.getBoolean() ? column
                : expression(column.type, List.of(column), Math.max(1, context.expressionDepth - 1), false);
        return aggregateArgument(argument);
    }

    private RisingWaveFunctionCall aggregateArgument(RisingWaveExpression argument) {
        RisingWaveType type = argument.getType();
        // Sums depend on accumulation order; MIN/MAX may choose different signed-zero representatives.
        if (type.floatingPoint()) {
            return count(argument);
        }
        if (type == RisingWaveType.TEXT && Randomly.getBoolean()) {
            // Ordering by the complete value and using one constant delimiter makes ties interchangeable.
            return new RisingWaveFunctionCall(RisingWaveAggregateFunction.STRING_AGG,
                    List.of(argument, new RisingWaveConstant(RisingWaveType.TEXT, Randomly.fromOptions(",", "|", ""))),
                    false, null, List.of(order(argument)));
        }
        if (type == RisingWaveType.BOOLEAN) {
            return new RisingWaveFunctionCall(
                    Randomly.fromOptions(RisingWaveAggregateFunction.BOOL_AND, RisingWaveAggregateFunction.BOOL_OR),
                    List.of(argument));
        }
        if (type.numeric() && Randomly.getBoolean()) {
            RisingWaveAggregateFunction function = Randomly.getBoolean() ? RisingWaveAggregateFunction.AVG
                    : RisingWaveAggregateFunction.SUM;
            return new RisingWaveFunctionCall(function, List.of(argument));
        }
        return new RisingWaveFunctionCall(
                Randomly.fromOptions(RisingWaveAggregateFunction.MIN, RisingWaveAggregateFunction.MAX),
                List.of(argument));
    }

    RisingWaveExpression subquery(RisingWaveType type, RisingWaveSubquery.Kind kind, RisingWaveExpression operand) {
        if (type.struct() && kind == RisingWaveSubquery.Kind.IN) {
            return subquery(type.fields().get(0).type, kind, new RisingWaveFieldAccess(operand, 0));
        }
        if ((type.floatingPoint() || type.array() || type.struct()) && kind == RisingWaveSubquery.Kind.SCALAR) {
            return literal(type);
        }
        if (kind == RisingWaveSubquery.Kind.SCALAR && --context.scalarSubqueryBudget < 0) {
            return literal(type);
        }
        RisingWaveTableReference source = context.table();
        RisingWaveSelect query = RisingWaveQueryGenerator.select(source);
        List<RisingWaveColumnReference> candidates = source.columns().stream().filter(c -> c.type == type)
                .collect(Collectors.toList());
        // Keep scalar aggregates bound to an eligible column in the subquery's own scope.
        if (kind == RisingWaveSubquery.Kind.SCALAR && candidates.isEmpty()) {
            return literal(type);
        }
        RisingWaveExpression value = candidates.isEmpty() ? literal(type) : Randomly.fromList(candidates);
        if (kind == RisingWaveSubquery.Kind.SCALAR) {
            // A global aggregate guarantees one row, without LIMIT or assumptions about input cardinality.
            value = new RisingWaveFunctionCall(type == RisingWaveType.BOOLEAN ? RisingWaveAggregateFunction.BOOL_OR
                    : RisingWaveAggregateFunction.MIN, List.of(value));
        }
        query.setFetchColumns(List.of(value));
        query.setWhereClause(
                expression(RisingWaveType.BOOLEAN, source.columns(), Math.max(0, context.expressionDepth - 2), false));
        RisingWaveExpression result = new RisingWaveSubquery(kind, query, operand);
        // The 3.1 planner treats bare boolean subqueries in WHERE as semi/anti joins.
        // An explicit comparison keeps scalar booleans on the scalar-subquery planning path.
        if (kind == RisingWaveSubquery.Kind.SCALAR && type == RisingWaveType.BOOLEAN) {
            return binary(Operator.EQUALS, RisingWaveType.BOOLEAN, result,
                    new RisingWaveConstant(RisingWaveType.BOOLEAN, "true"));
        }
        return result;
    }

    RisingWaveExpression existsFilter(RisingWaveType type, List<RisingWaveColumnReference> outerScope) {
        if (context.correlatedExistsBudget > 0 && context.expressionBudget >= 3 && context.relationBudget > 0
                && (context.streaming || Randomly.fromOptions(false, false, false, true))) {
            List<RisingWaveTable> eligible = context.tables.stream()
                    .filter(t -> t.getColumns().stream()
                            .anyMatch(c -> !c.getType().array() && !c.getType().struct()
                                    && outerScope.stream().anyMatch(o -> o.type == c.getType())))
                    .collect(Collectors.toList());
            if (!eligible.isEmpty()) {
                RisingWaveTableReference local = new RisingWaveTableReference(Randomly.fromList(eligible),
                        context.alias());
                List<RisingWaveColumnReference> localKeys = local.columns().stream()
                        .filter(c -> !c.type.array() && !c.type.struct())
                        .filter(c -> outerScope.stream().anyMatch(o -> o.type == c.type)).collect(Collectors.toList());
                RisingWaveColumnReference inner = Randomly.fromList(localKeys);
                RisingWaveColumnReference outer = Randomly
                        .fromList(outerScope.stream().filter(c -> c.type == inner.type).collect(Collectors.toList()));
                context.correlatedExistsBudget--;
                context.expressionBudget -= 3;
                context.relationBudget--;
                RisingWaveSelect query = RisingWaveQueryGenerator.select(local);
                query.setFetchColumns(List.of(new RisingWaveConstant(RisingWaveType.INT, "1")));
                RisingWaveExpression predicate = binary(Operator.EQUALS, RisingWaveType.BOOLEAN, inner, outer);
                if (Randomly.getBoolean()) {
                    predicate = binary(Operator.AND, RisingWaveType.BOOLEAN, predicate, expression(
                            RisingWaveType.BOOLEAN, local.columns(), Math.max(0, context.expressionDepth - 1), false));
                }
                query.setWhereClause(predicate);
                return new RisingWaveSubquery(RisingWaveSubquery.Kind.EXISTS, query, null, List.of(outer));
            }
        }
        return context.streaming ? expression(RisingWaveType.BOOLEAN, outerScope, 0, false)
                : subquery(type, RisingWaveSubquery.Kind.EXISTS, null);
    }

    public RisingWaveExpression expression(RisingWaveType type, List<RisingWaveColumnReference> scope, int depth,
            boolean allowSubqueries) {
        boolean subqueries = allowSubqueries && !context.streaming;
        if (context.streaming && (type.numeric() || type == RisingWaveType.TIME)) {
            // Persisted streaming expressions must remain evaluable for boundary-valued later INSERTs.
            return leaf(type, scope);
        }
        if (--context.expressionBudget <= 0 || depth >= context.expressionDepth
                || Randomly.getBooleanWithRatherLowProbability()) {
            return leaf(type, scope);
        }
        int next = depth + 1;
        switch (Randomly.fromOptions(ExpressionKind.values())) {
        case CASE:
            return new RisingWaveCase(expression(RisingWaveType.BOOLEAN, scope, next, subqueries),
                    expression(type, scope, next, subqueries), expression(type, scope, next, subqueries));
        case COALESCE:
            return new RisingWaveFunctionCall("coalesce", type,
                    List.of(expression(type, scope, next, subqueries), expression(type, scope, next, subqueries)));
        case SCALAR_FUNCTION:
            return scalarFunction(type, scope, next, subqueries);
        case COMPARISON_OR_ARITHMETIC:
            return type == RisingWaveType.BOOLEAN ? comparison(scope, next, subqueries)
                    : arithmetic(type, scope, next, subqueries);
        case LOGICAL_OR_CAST:
            if (type == RisingWaveType.BOOLEAN) {
                return binary(Randomly.fromOptions(Operator.AND, Operator.OR), type,
                        expression(type, scope, next, subqueries), expression(type, scope, next, subqueries));
            }
            return cast(type, scope, next, subqueries);
        case UNARY_OR_CAST:
            if (type == RisingWaveType.BOOLEAN) {
                return Randomly.getBoolean() ? negatePredicate(expression(type, scope, next, subqueries))
                        : isNull(expression(Randomly.fromOptions(RisingWaveType.values()), scope, next, subqueries));
            }
            return cast(type, scope, next, subqueries);
        case SCALAR_SUBQUERY:
            if (subqueries) {
                return subquery(type, RisingWaveSubquery.Kind.SCALAR, null);
            }
            return leaf(type, scope);
        case ARRAY_ACCESS:
            // In 3.1, EXISTS under scalar boolean operations can lose outer rows for empty input.
            // Query-block generation emits EXISTS only as a direct filter; keep this context scalar.
            RisingWaveExpression arrayValue = arrayScalar(type, scope);
            return arrayValue == null ? leaf(type, scope) : arrayValue;
        case BOOLEAN_PREDICATE:
            return booleanPredicate(type, scope, next, subqueries);
        default:
            throw new AssertionError();
        }
    }

    private RisingWaveExpression scalarFunction(RisingWaveType type, List<RisingWaveColumnReference> scope, int next,
            boolean subqueries) {
        List<RisingWaveScalarFunction> functions = RisingWaveScalarFunction.returning(type);
        if (!functions.isEmpty()) {
            RisingWaveScalarFunction function = Randomly.fromList(functions);
            List<RisingWaveExpression> arguments = new ArrayList<>();
            for (int i = 0; i < function.arguments.size(); i++) {
                // Bound text-slice lengths independently of numeric expressions and table values.
                arguments.add(i == 1
                        && (function == RisingWaveScalarFunction.LEFT || function == RisingWaveScalarFunction.RIGHT)
                                ? new RisingWaveConstant(RisingWaveType.INT,
                                        Long.toString(Randomly.getNotCachedInteger(0, 9)))
                                : expression(function.arguments.get(i), scope, next, subqueries));
            }
            return new RisingWaveFunctionCall(function.sql, function.result, arguments);
        }
        return leaf(type, scope);
    }

    private RisingWaveExpression comparison(List<RisingWaveColumnReference> scope, int depth, boolean subqueries) {
        RisingWaveType compared = !scope.isEmpty() && Randomly.getBoolean() ? outputType(Randomly.fromList(scope))
                : Randomly.fromOptions(RisingWaveType.values());
        if (compared.struct()) {
            return isNull(expression(compared, scope, depth, subqueries));
        }
        return binary(comparisonOperator(compared), RisingWaveType.BOOLEAN,
                expression(compared, scope, depth, subqueries), expression(compared, scope, depth, subqueries));
    }

    private static Operator comparisonOperator(RisingWaveType type) {
        return type.array() ? Randomly.fromOptions(Operator.EQUALS, Operator.NOT_EQUALS)
                : Randomly.fromOptions(Operator.EQUALS, Operator.NOT_EQUALS, Operator.LESS, Operator.LESS_EQUALS,
                        Operator.GREATER, Operator.GREATER_EQUALS);
    }

    private RisingWaveExpression arithmetic(RisingWaveType type, List<RisingWaveColumnReference> scope, int next,
            boolean subqueries) {
        if (type == RisingWaveType.TIME) {
            return new RisingWaveOperation(Randomly.fromOptions(Operator.ADD, Operator.SUBTRACT), type,
                    expression(type, scope, next, subqueries),
                    new RisingWaveConstant(RisingWaveType.INTERVAL, "1 second"));
        } else if (type == RisingWaveType.INT && Randomly.getBoolean()) {
            // A positive divisor avoids zero-division and MIN/-1 remainder overflow without narrowing the input.
            return binary(Operator.MODULO, type, expression(type, scope, next, subqueries),
                    new RisingWaveConstant(type, Long.toString(Randomly.getNotCachedInteger(1, 9))));
        } else if (type.numeric() && !type.floatingPoint() || type == RisingWaveType.INTERVAL) {
            Operator operator = type.numeric()
                    ? Randomly.fromOptions(Operator.ADD, Operator.SUBTRACT, Operator.MULTIPLY)
                    : Randomly.fromOptions(Operator.ADD, Operator.SUBTRACT);
            return binary(operator, type, expression(type, scope, next, subqueries),
                    expression(type, scope, next, subqueries));
        } else if (type == RisingWaveType.TEXT) {
            return binary(Operator.CONCAT, type, expression(type, scope, next, subqueries),
                    expression(type, scope, next, subqueries));
        }
        return leaf(type, scope);
    }

    private RisingWaveExpression booleanPredicate(RisingWaveType type, List<RisingWaveColumnReference> scope, int next,
            boolean subqueries) {
        if (type != RisingWaveType.BOOLEAN) {
            return leaf(type, scope);
        }
        RisingWaveType operandType = scope.isEmpty() ? RisingWaveType.INT : Randomly.fromList(scope).type;
        RisingWaveExpression operand = expression(operandType, scope, next, subqueries);
        if (operandType.struct()) {
            return isNull(operand);
        }
        if (operandType.array()) {
            return binary(Operator.EQUALS, RisingWaveType.BOOLEAN, operand,
                    expression(operandType, scope, next, subqueries));
        }
        switch (Randomly.fromOptions(PredicateKind.values())) {
        case LIKE:
            return binary(Operator.LIKE, type, expression(RisingWaveType.TEXT, scope, next, subqueries),
                    new RisingWaveConstant(RisingWaveType.TEXT, Randomly.fromOptions("%", "v%", "_", "same", "雪%")));
        case BETWEEN:
            return new RisingWaveOperation(Randomly.fromOptions(Operator.BETWEEN, Operator.NOT_BETWEEN), type, operand,
                    expression(operandType, scope, next, subqueries), expression(operandType, scope, next, subqueries));
        case IN:
            return new RisingWaveOperation(Randomly.fromOptions(Operator.IN, Operator.NOT_IN), type, operand,
                    expression(operandType, scope, next, subqueries), expression(operandType, scope, next, subqueries));
        default:
            throw new AssertionError();
        }
    }

    private RisingWaveExpression cast(RisingWaveType target, List<RisingWaveColumnReference> scope, int depth,
            boolean subqueries) {
        RisingWaveType source;
        switch (target) {
        case BIGINT_ARRAY:
            source = RisingWaveType.INT_ARRAY;
            break;
        case REAL:
            source = Randomly.fromOptions(RisingWaveType.SMALLINT, RisingWaveType.INT);
            break;
        case DOUBLE:
            source = Randomly.fromOptions(RisingWaveType.REAL, RisingWaveType.INT);
            break;
        case SMALLINT:
            // Unrestricted narrowing has a distinct cast-error message; keep generated inputs in range.
            return new RisingWaveCast(
                    new RisingWaveConstant(RisingWaveType.INT, Randomly.fromOptions("-32768", "32767", "0", "1")),
                    target);
        case INT:
            source = Randomly.fromOptions(RisingWaveType.SMALLINT, RisingWaveType.BOOLEAN);
            break;
        case TIME:
            source = RisingWaveType.TIMESTAMP;
            break;
        case DECIMAL:
            source = Randomly.fromOptions(RisingWaveType.SMALLINT, RisingWaveType.INT, RisingWaveType.BIGINT);
            break;
        case BIGINT:
            source = Randomly.fromOptions(RisingWaveType.SMALLINT, RisingWaveType.INT);
            break;
        case TIMESTAMP:
            source = RisingWaveType.DATE;
            break;
        case DATE:
            source = RisingWaveType.TIMESTAMP;
            break;
        case TEXT:
            // Text exposes signed-zero representation, unlike SQL floating equality.
            source = Randomly.fromList(Arrays.stream(RisingWaveType.values())
                    .filter(t -> !t.floatingPoint() && !t.array() && !t.struct()).collect(Collectors.toList()));
            break;
        default:
            source = target;
        }
        return new RisingWaveCast(expression(source, scope, depth, subqueries), target);
    }

    RisingWaveExpression leaf(RisingWaveType type, List<RisingWaveColumnReference> scope) {
        List<RisingWaveExpression> fields = new ArrayList<>();
        for (RisingWaveColumnReference owner : scope) {
            for (int i = 0; i < owner.type.fields().size(); i++) {
                if (owner.type.fields().get(i).type == type) {
                    fields.add(new RisingWaveFieldAccess(owner, i));
                }
            }
        }
        boolean directColumn = scope.stream().anyMatch(c -> c.type == type);
        if (!fields.isEmpty() && (directColumn ? Randomly.getBooleanWithRatherLowProbability()
                : Randomly.fromOptions(true, true, true, false))) {
            return Randomly.fromList(fields);
        }
        List<RisingWaveColumnReference> columns = scope.stream().filter(c -> c.type == type)
                .collect(Collectors.toList());
        if (!columns.isEmpty() && Randomly.fromOptions(true, true, true, false)) {
            return Randomly.fromList(columns);
        }
        if (!type.array() && scope.stream().anyMatch(c -> c.type.array()) && Randomly.getBoolean()) {
            RisingWaveExpression fromArray = arrayScalar(type, scope);
            if (fromArray != null) {
                return fromArray;
            }
        }
        if (type == RisingWaveType.BOOLEAN && columns.isEmpty() && !scope.isEmpty() && Randomly.getBoolean()) {
            RisingWaveColumnReference column = Randomly.fromList(scope);
            if (column.type.struct()) {
                return isNull(column);
            }
            return binary(comparisonOperator(column.type), RisingWaveType.BOOLEAN, column, literal(column.type));
        }
        return literal(type);
    }

    static RisingWaveType outputType(RisingWaveColumnReference column) {
        // Keep complex-valued outputs reachable alongside scalar consumers of their exposed data.
        if (column.type.struct() && Randomly.getBoolean()) {
            return Randomly.fromList(column.type.fields()).type;
        }
        if (column.type.array() && Randomly.getBoolean()) {
            return Randomly.getBoolean() ? column.type.elementType() : RisingWaveType.INT;
        }
        return column.type;
    }

    RisingWaveExpression literal(RisingWaveType type) {
        return RisingWaveLiteralGenerator.literal(type);
    }

    private RisingWaveExpression arrayScalar(RisingWaveType type, List<RisingWaveColumnReference> scope) {
        List<RisingWaveColumnReference> arrays = scope.stream()
                .filter(c -> c.type.array() && (type == RisingWaveType.INT || c.type.elementType() == type))
                .collect(Collectors.toList());
        if (arrays.isEmpty()) {
            return null;
        }
        RisingWaveColumnReference array = Randomly.fromList(arrays);
        if (type == RisingWaveType.INT && (array.type.elementType() != type || Randomly.getBoolean())) {
            return new RisingWaveFunctionCall("cardinality", RisingWaveType.INT, List.of(array));
        }
        return new RisingWaveArrayAccess(array,
                new RisingWaveConstant(RisingWaveType.INT, Long.toString(Randomly.getNotCachedInteger(-1, 7))));
    }

    RisingWaveExpression joinComparison(RisingWaveExpression left, List<RisingWaveColumnReference> rightColumns) {
        // Use equality for streaming joins.
        Operator operator = context.streaming ? Operator.EQUALS : Randomly.fromOptions(Operator.EQUALS, Operator.EQUALS,
                Operator.EQUALS, Operator.NOT_EQUALS, Operator.LESS, Operator.GREATER_EQUALS, Operator.LESS_EQUALS);
        return binary(operator, RisingWaveType.BOOLEAN, left, Randomly.fromList(rightColumns));
    }

    RisingWaveExpression havingComparison(RisingWaveExpression argument) {
        return binary(
                Randomly.fromOptions(Operator.EQUALS, Operator.NOT_EQUALS, Operator.GREATER, Operator.LESS_EQUALS),
                RisingWaveType.BOOLEAN, argument, literal(argument.getType()));
    }

    RisingWaveExpression binary(Operator operator, RisingWaveType type, RisingWaveExpression left,
            RisingWaveExpression right) {
        if (left.getType() != right.getType()) {
            throw new IllegalArgumentException("Binary operands must have matching types");
        }
        return new RisingWaveOperation(operator, type, left, right);
    }

    // expression() already charges for these operators. TLP adds them after generating the predicate.
    public RisingWaveExpression negatePredicate(RisingWaveExpression predicate) {
        return new RisingWaveOperation(Operator.NOT, RisingWaveType.BOOLEAN, predicate);
    }

    public RisingWaveExpression isNull(RisingWaveExpression expression) {
        return new RisingWaveOperation(Operator.IS_NULL, RisingWaveType.BOOLEAN, expression);
    }
}
