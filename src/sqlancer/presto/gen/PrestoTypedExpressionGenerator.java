package sqlancer.presto.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoAggregateFunction;
import sqlancer.presto.ast.PrestoAtTimeZoneOperator;
import sqlancer.presto.ast.PrestoBetweenOperation;
import sqlancer.presto.ast.PrestoBinaryOperation;
import sqlancer.presto.ast.PrestoCaseOperation;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoColumnReference;
import sqlancer.presto.ast.PrestoConstant;
import sqlancer.presto.ast.PrestoDefaultFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoFunctionNode;
import sqlancer.presto.ast.PrestoInOperation;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoMultiValuedComparison;
import sqlancer.presto.ast.PrestoMultiValuedComparisonOperator;
import sqlancer.presto.ast.PrestoMultiValuedComparisonType;
import sqlancer.presto.ast.PrestoPostfixText;
import sqlancer.presto.ast.PrestoQuantifiedComparison;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.ast.PrestoTableReference;
import sqlancer.presto.ast.PrestoTernary;
import sqlancer.presto.ast.PrestoUnaryPostfixOperation;
import sqlancer.presto.ast.PrestoUnaryPrefixOperation;

public final class PrestoTypedExpressionGenerator extends
        TypedExpressionGenerator<PrestoExpression, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType>
        implements NoRECGenerator<PrestoSelect, PrestoJoin, PrestoExpression, PrestoTable, PrestoColumn>,
        TLPWhereGenerator<PrestoSelect, PrestoJoin, PrestoExpression, PrestoTable, PrestoColumn> {

    private final Randomly randomly;
    private final PrestoGlobalState globalState;
    private final int maxDepth;
    private List<PrestoTable> tables;

    public PrestoTypedExpressionGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.randomly = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    @Override
    public PrestoExpression generatePredicate() {
        return generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN),
                randomly.getInteger(0, maxDepth));
    }

    @Override
    public PrestoExpression negatePredicate(PrestoExpression predicate) {
        return new PrestoUnaryPrefixOperation(predicate, PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT);
    }

    @Override
    public PrestoExpression isNull(PrestoExpression expr) {
        return new PrestoUnaryPostfixOperation(expr, PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public PrestoExpression generateConstant(PrestoSchema.PrestoCompositeDataType type) {
        if (Objects.requireNonNull(type.getPrimitiveDataType()) == PrestoSchema.PrestoDataType.ARRAY) {
            return PrestoConstant.createArrayConstant(type);
            // case MAP:
            // return PrestoConstant.createMapConstant(type);
        }
        return PrestoConstant.generateConstant(type, false);
    }

    public PrestoExpression generateInsertConstant(PrestoSchema.PrestoCompositeDataType type) {
        if (Objects.requireNonNull(type.getPrimitiveDataType()) == PrestoSchema.PrestoDataType.ARRAY) {
            return PrestoConstant.createArrayConstant(type);
            // case MAP:
            // return PrestoConstant.createMapConstant(type);
        }
        return PrestoConstant.generateConstant(type, true);
    }

    @Override
    public PrestoExpression generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (allowAggregates && Randomly.getBoolean()) {
            return generateAggregate(type);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        } else {
            // TODO: functions
            List<PrestoDefaultFunction> applicableFunctions = PrestoDefaultFunction.getFunctionsCompatibleWith(type);
            if (Randomly.getBooleanWithRatherLowProbability() && !applicableFunctions.isEmpty()) {
                PrestoDefaultFunction function = Randomly.fromList(applicableFunctions);
                return generateFunction(type, depth, function);
            }
            // TODO: try
            // if (Randomly.getBooleanWithRatherLowProbability()) {
            // return generateTry(type, depth);
            // }

            // TODO: cast
            //
            // if (Randomly.getBooleanWithRatherLowProbability()) {
            // Node<PrestoExpression> expressionNode = generateCast(type, depth);
            // }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return getCase(type, depth);
            }
            switch (type.getPrimitiveDataType()) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case VARCHAR:
            case CHAR:
                return generateStringExpression(type, depth);
            case INT:
            case DECIMAL:
            case FLOAT:
                return generateNumericExpression(depth);
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return generateTemporalExpression(type, depth);
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
                return generateIntervalExpression(type, depth);
            case JSON:
                return generateJsonExpression(type);
            case VARBINARY:
            case ARRAY:
                // case MAP:
                return generateLeafNode(type); // TODO
            default:
                throw new AssertionError(type);
            }
        }
    }

    private PrestoExpression generateJsonExpression(PrestoSchema.PrestoCompositeDataType type) {
        return generateLeafNode(type);
    }

    private PrestoExpression generateCast(PrestoSchema.PrestoCompositeDataType type, int depth) {
        // check can cast
        PrestoExpression expressionNode = generateExpression(getRandomType(), depth + 1);
        return new PrestoCastFunction(expressionNode, type);
    }

    @SuppressWarnings("unused")
    private PrestoExpression generateTry(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (type.getPrimitiveDataType().isNumeric() && Randomly.getBooleanWithRatherLowProbability()) {
            PrestoExpression expression = generateExpression(type);
            return new PrestoFunctionNode<>(List.of(expression), "try");
        }

        List<PrestoDefaultFunction> applicableFunctions = PrestoDefaultFunction.getFunctionsCompatibleWith(type);
        if (Randomly.getBooleanWithRatherLowProbability() && !applicableFunctions.isEmpty()) {
            PrestoDefaultFunction function = Randomly.fromList(applicableFunctions);
            PrestoExpression expression = generateFunction(type, depth, function);
            return new PrestoFunctionNode<>(List.of(expression), "try");
        }
        return new PrestoFunctionNode<>(List.of(generateCast(type, depth)), "try");
    }

    private PrestoCaseOperation getCase(PrestoSchema.PrestoCompositeDataType type, int depth) {
        List<PrestoExpression> conditions = new ArrayList<>();
        List<PrestoExpression> cases = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            conditions.add(generateExpression(type, depth + 1));
            cases.add(generateExpression(type, depth + 1));
        }
        PrestoExpression elseExpr = null;
        if (Randomly.getBoolean()) {
            elseExpr = generateExpression(type, depth + 1);
        }
        PrestoExpression expression = generateExpression(type);
        return new PrestoCaseOperation(expression, conditions, cases, elseExpr);
    }

    private PrestoExpression generateFunction(PrestoSchema.PrestoCompositeDataType returnType, int depth,
            PrestoDefaultFunction function) {

        PrestoSchema.PrestoDataType[] argumentTypes = function.getArgumentTypes(returnType);
        List<PrestoExpression> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to
        // the same element type.
        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
        if (returnType.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY) {
            savedArrayType = returnType;
        }
        if (function.getNumberOfArguments() == -1) {
            PrestoSchema.PrestoDataType dataType = argumentTypes[0];
            // TODO: consider upper
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                PrestoSchema.PrestoCompositeDataType type;

                if (dataType == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = dataType.get();
                    }
                    type = savedArrayType;
                } else {
                    type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
                }
                arguments.add(generateExpression(type, depth + 1));
            }
        } else {
            for (PrestoSchema.PrestoDataType arg : argumentTypes) {
                PrestoSchema.PrestoCompositeDataType dataType;
                if (arg == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = arg.get();
                    }
                    dataType = savedArrayType;
                } else {
                    dataType = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
                }
                PrestoExpression expression = generateExpression(dataType, depth + 1);
                arguments.add(expression);
            }
        }
        return new PrestoFunctionNode<>(arguments, function);
    }

    private PrestoExpression generateStringExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }
        return getStringOperation(depth);
    }

    private PrestoBinaryOperation getStringOperation(int depth) {
        StringExpression exprType = Randomly.fromOptions(StringExpression.values());
        if (Objects.requireNonNull(exprType) == StringExpression.CONCAT) {
            PrestoExpression left = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
            PrestoExpression right = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
            PrestBinaryStringOperator operator = PrestBinaryStringOperator.CONCAT;
            return new PrestoBinaryOperation(left, right, operator);
        }
        throw new AssertionError(exprType);
    }

    private PrestoExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> booleanExpressions = Arrays.stream(BooleanExpression.values())
                .collect(Collectors.toList());
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            booleanExpressions.remove(BooleanExpression.BETWEEN);
        }

        booleanExpressions.remove(BooleanExpression.REGEX);

        BooleanExpression exprType = Randomly.fromList(booleanExpressions);
        switch (exprType) {
        case NOT:
            return generateNOT(depth + 1);
        case BINARY_COMPARISON:
            return getBinaryComparison(depth);
        case BINARY_LOGICAL:
            return getBinaryLogical(depth);
        case AND_OR_CHAIN:
            return getAndOrChain(depth);
        case REGEX:
            return getRegex(depth);
        case IS_NULL:
            return new PrestoUnaryPostfixOperation(generateExpression(getRandomType(), depth + 1),
                    Randomly.fromOptions(PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL,
                            PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NOT_NULL));
        case IN:
            return getInOperation(depth);
        case BETWEEN:
            return getBetween(depth);
        case LIKE:
            return getLike(depth);
        case MULTI_VALUED_COMPARISON: // TODO other operators
            return getMultiValuedComparison(depth);
        default:
            throw new AssertionError(exprType);
        }
    }

    private PrestoExpression getMultiValuedComparison(int depth) {

        PrestoSchema.PrestoCompositeDataType type;
        do {
            type = PrestoSchema.PrestoCompositeDataType
                    .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getOrderableTypes()));
        } while (type.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY
                && !type.getElementType().getPrimitiveDataType().isOrderable());

        PrestoMultiValuedComparisonType comparisonType = PrestoMultiValuedComparisonType.getRandom();
        PrestoMultiValuedComparisonOperator comparisonOperator = PrestoMultiValuedComparisonOperator
                .getRandomForType(type);
        PrestoExpression left = generateExpression(type, depth + 1);
        // sub-query
        PrestoSchema.PrestoCompositeDataType finalType = type;
        List<PrestoSchema.PrestoColumn> columnsOfType = columns.stream().filter(c -> c.getType() == finalType)
                .collect(Collectors.toList());
        if (Randomly.getBooleanWithRatherLowProbability() && !columnsOfType.isEmpty()) {
            PrestoSchema.PrestoColumn column = Randomly.fromList(columnsOfType);
            PrestoSelect subquery = generateSubquery(List.of(column));
            return new PrestoQuantifiedComparison(left, subquery, comparisonType, comparisonOperator);
        }
        int nr = Randomly.smallNumber() + 2;
        List<PrestoExpression> rightList = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            rightList.add(generateConstant(type));
        }
        return new PrestoMultiValuedComparison(left, rightList, comparisonType, comparisonOperator);
    }

    private PrestoSelect generateSubquery(List<PrestoSchema.PrestoColumn> columns) {
        PrestoSelect select = new PrestoSelect();
        List<PrestoExpression> allColumns = columns.stream().map((c) -> new PrestoColumnReference(c))
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        List<PrestoSchema.PrestoTable> tables = columns.stream().map(AbstractTableColumn::getTable)
                .collect(Collectors.toList());
        List<PrestoTableReference> tableList = tables.stream().map(t -> new PrestoTableReference(t)).distinct()
                .collect(Collectors.toList());
        List<PrestoExpression> tableNodeList = tables.stream().map(t -> new PrestoTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableNodeList);
        TypedExpressionGenerator<PrestoExpression, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> typedExpressionGenerator = new PrestoTypedExpressionGenerator(
                globalState).setColumns(columns);
        PrestoExpression predicate = typedExpressionGenerator.generatePredicate();
        select.setWhereClause(predicate);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(typedExpressionGenerator.generateOrderBys());
        }
        List<PrestoExpression> joins = PrestoJoin.getJoins(tableList, globalState).stream()
                .collect(Collectors.toList());
        select.setJoinList(joins);
        return select;
    }

    private PrestoExpression generateNumericExpression(int depth) {
        PrestoSchema.PrestoDataType dataType = Randomly.fromList(PrestoSchema.PrestoDataType.getNumberTypes());
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
        if (Randomly.getBoolean()) {
            BinaryOperatorNode.Operator operator = PrestoBinaryArithmeticOperator.getRandom();
            PrestoExpression left = generateExpression(type, depth);
            PrestoExpression right = generateExpression(type, depth);
            return new PrestoBinaryOperation(left, right, operator);
        } else {
            BinaryOperatorNode.Operator operator = PrestoUnaryArithmeticOperator.MINUS;
            PrestoExpression left = generateExpression(type, depth);
            return new PrestoUnaryPrefixOperation(left, operator);
        }
    }

    private PrestoExpression generateTemporalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            PrestoExpression left = generateExpression(type, depth);
            PrestoExpression right = generateExpression(PrestoSchema.PrestoCompositeDataType
                    .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getIntervalTypes())), depth);
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            return new PrestoBinaryOperation(left, right, operator);
        }

        // timestamp at time zone
        if (Randomly.getBooleanWithSmallProbability()
                && (type.getPrimitiveDataType() == PrestoSchema.PrestoDataType.TIMESTAMP
                        || type.getPrimitiveDataType() == PrestoSchema.PrestoDataType.TIMESTAMP_WITH_TIME_ZONE)) {
            return new PrestoAtTimeZoneOperator(generateExpression(type, depth + 1),
                    PrestoConstant.createTimezoneConstant());
        }
        return generateLeafNode(type);
    }

    private PrestoExpression generateIntervalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            PrestoExpression left = generateExpression(type, depth);

            PrestoExpression right;
            if (Randomly.getBoolean()) {
                right = generateExpression(PrestoSchema.PrestoCompositeDataType
                        .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getTemporalTypes())), depth);
            } else {
                right = generateExpression(type, depth);
            }
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            if (Randomly.getBoolean()) {
                return new PrestoBinaryOperation(left, right, operator);
            } else {
                return new PrestoBinaryOperation(right, left, operator);
            }
        }
        return generateLeafNode(type);

        // functions

        // timestamp at time zone
    }

    private PrestoExpression getLike(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.VARCHAR);
        PrestoExpression expression = generateExpression(type, depth + 1);
        PrestoExpression pattern = generateExpression(type, depth + 1);
        if (Randomly.getBoolean()) {
            return new PrestoBinaryOperation(expression, pattern, PrestoLikeOperator.getRandom());
        } else {
            String randomlyString = randomly.getString();
            String randomlyChar = randomly.getChar();
            PrestoExpression escape = new PrestoConstant.PrestoTextConstant(randomlyChar, 1);
            int index = randomlyString.indexOf(randomlyChar);
            while (index > -1) {
                String wildcard = Randomly.fromOptions("%", "_");
                randomlyString = randomlyString.substring(0, index + 1) + wildcard
                        + randomlyString.substring(index + 1);
                index = randomlyString.indexOf(randomlyChar, index + 1);
            }
            PrestoConstant.PrestoTextConstant patternString = new PrestoConstant.PrestoTextConstant(randomlyString);
            return new PrestoTernary(expression, patternString, escape, "LIKE", "ESCAPE");
        }
    }

    private PrestoBinaryOperation getRegex(int depth) {
        PrestoExpression left = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
        PrestoExpression right = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
        return new PrestoBinaryOperation(left, right, PrestoBinaryLogicalOperator.getRandom());
    }

    private PrestoBinaryOperation getBinaryLogical(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.BOOLEAN);
        PrestoExpression left = generateExpression(type, depth + 1);
        PrestoExpression right = generateExpression(type, depth + 1);
        BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
        return new PrestoBinaryOperation(left, right, operator);
    }

    private PrestoExpression getBetween(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getNumericTypes()));
        PrestoExpression expression = generateExpression(type, depth + 1);
        PrestoExpression left = generateExpression(type, depth + 1);
        PrestoExpression right = generateExpression(type, depth + 1);
        return new PrestoBetweenOperation(expression, left, right, Randomly.getBoolean());
    }

    private PrestoExpression getInOperation(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        PrestoExpression left = generateExpression(type, depth + 1);
        List<PrestoExpression> inList = generateExpressions(type, Randomly.smallNumber() + 1, depth + 1);
        boolean isNegated = Randomly.getBoolean();
        return new PrestoInOperation(left, inList, isNegated);
    }

    private PrestoExpression getAndOrChain(int depth) {
        PrestoExpression left = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth + 1);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            PrestoExpression right = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth + 1);
            BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
            left = new PrestoBinaryOperation(left, right, operator);
        }
        return left;
    }

    private PrestoExpression getBinaryComparison(int depth) {
        PrestoSchema.PrestoCompositeDataType type = getRandomType();
        BinaryOperatorNode.Operator op = PrestoBinaryComparisonOperator.getRandomForType(type);
        PrestoExpression left = generateExpression(type, depth + 1);
        PrestoExpression right = generateExpression(type, depth + 1);
        return new PrestoBinaryOperation(left, right, op);
    }

    private PrestoExpression generateNOT(int depth) {
        PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator operator = PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT;
        return new PrestoUnaryPrefixOperation(
                generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth),
                operator);
    }

    @Override
    protected PrestoExpression generateColumn(PrestoSchema.PrestoCompositeDataType type) {
        List<PrestoSchema.PrestoColumn> columnList = columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == type.getPrimitiveDataType())
                .collect(Collectors.toList());
        PrestoSchema.PrestoColumn column = Randomly.fromList(columnList);
        return new PrestoColumnReference(column);
    }

    @Override
    public PrestoExpression generateLeafNode(PrestoSchema.PrestoCompositeDataType type) {
        if (Randomly.getBoolean()) {
            return generateConstant(type);
        } else {
            List<PrestoSchema.PrestoColumn> columnList = filterColumns(type.getPrimitiveDataType());
            if (columnList.isEmpty()) {
                return generateConstant(type);
            } else {
                return generateColumn(type);
            }
        }
    }

    private List<PrestoSchema.PrestoColumn> filterColumns(PrestoSchema.PrestoDataType dataType) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType().getPrimitiveDataType() == dataType)
                    .collect(Collectors.toList());
        }
    }

    @Override
    protected PrestoSchema.PrestoCompositeDataType getRandomType() {
        return PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull();
    }

    @Override
    protected boolean canGenerateColumnOfType(PrestoSchema.PrestoCompositeDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    public PrestoExpression generateAggregate() {
        PrestoAggregateFunction aggregateFunction = PrestoAggregateFunction.getRandom();
        List<PrestoExpression> argsForAggregate = generateArgsForAggregate(aggregateFunction);
        return new PrestoFunctionNode<>(argsForAggregate, aggregateFunction);
    }

    public List<PrestoExpression> generateArgsForAggregate(PrestoAggregateFunction aggregateFunction) {
        PrestoSchema.PrestoCompositeDataType returnType;
        do {
            returnType = aggregateFunction.getCompositeReturnType();
        } while (!aggregateFunction.isCompatibleWithReturnType(returnType));
        return aggregateFunction.getArgumentsForReturnType(this, this.maxDepth - 1, returnType, false);
    }

    private PrestoExpression generateAggregate(PrestoSchema.PrestoCompositeDataType type) {
        PrestoAggregateFunction aggregateFunction = Randomly
                .fromList(PrestoAggregateFunction.getFunctionsCompatibleWith(type));
        List<PrestoExpression> argsForAggregate = generateArgsForAggregate(type, aggregateFunction);
        return new PrestoFunctionNode<>(argsForAggregate, aggregateFunction);
    }

    public List<PrestoExpression> generateArgsForAggregate(PrestoSchema.PrestoCompositeDataType type,
            PrestoAggregateFunction aggregateFunction) {
        List<PrestoSchema.PrestoDataType> returnTypes = aggregateFunction.getReturnTypes(type.getPrimitiveDataType());
        List<PrestoExpression> arguments = new ArrayList<>();
        allowAggregates = false; //
        for (PrestoSchema.PrestoDataType argumentType : returnTypes) {
            arguments.add(generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(argumentType)));
        }
        // return new NewFunctionNode<>(arguments, aggregateFunction);
        return arguments;
    }

    @Override
    public List<PrestoExpression> generateOrderBys() {
        List<PrestoExpression> expressions = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        ArrayList<PrestoSchema.PrestoColumn> prestoColumns = new ArrayList<>(columns);
        prestoColumns.removeIf(c -> !c.isOrderable());
        for (int i = 0; i < nr && !prestoColumns.isEmpty(); i++) {
            PrestoSchema.PrestoColumn randomColumn = Randomly.fromList(prestoColumns);
            PrestoColumnReference columnReference = new PrestoColumnReference(randomColumn);
            prestoColumns.remove(randomColumn);
            expressions.add(columnReference);
        }
        return expressions;
    }

    public PrestoExpression generateHavingClause() {
        allowAggregates = true;
        PrestoExpression expr = generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
        allowAggregates = false;
        return expr;
    }

    public PrestoExpression generateExpressionWithColumns(List<PrestoSchema.PrestoColumn> columns, int remainingDepth) {
        if (columns.isEmpty() || remainingDepth <= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            return generateConstant(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
        }
        PrestoSchema.PrestoColumn column = Randomly.fromList(columns);
        if (remainingDepth <= 2 || Randomly.getBooleanWithRatherLowProbability()) {
            return new PrestoColumnReference(column);
        }
        List<Expression> possibleOptions = new ArrayList<>(
                Arrays.asList(PrestoTypedExpressionGenerator.Expression.values()));
        PrestoTypedExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        BinaryOperatorNode.Operator op;
        switch (expr) {
        case BINARY_LOGICAL:
        case BINARY_ARITHMETIC:
            op = PrestoTypedExpressionGenerator.PrestoBinaryLogicalOperator.getRandom();
            break;
        case BINARY_COMPARISON:
            op = PrestoBinaryComparisonOperator.getRandom();
            break;
        default:
            throw new AssertionError();
        }
        return new PrestoBinaryOperation(generateExpression(column.getType(), remainingDepth - 1),
                generateExpression(column.getType(), remainingDepth - 1), op);
    }

    private enum StringExpression {
        CONCAT
    }

    public enum PrestBinaryStringOperator implements BinaryOperatorNode.Operator {
        CONCAT("||");

        private final String textRepresentation;

        PrestBinaryStringOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoBinaryTemporalOperator implements BinaryOperatorNode.Operator {
        ADD("+"), SUB("-");

        private final String textRepresentation;

        PrestoBinaryTemporalOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    private enum BooleanExpression {
        NOT, BINARY_COMPARISON, BINARY_LOGICAL, AND_OR_CHAIN, REGEX, IS_NULL, IN, BETWEEN, LIKE, MULTI_VALUED_COMPARISON
    }

    public enum PrestoBinaryLogicalOperator implements BinaryOperatorNode.Operator {

        AND, OR;

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    public enum PrestoLikeOperator implements BinaryOperatorNode.Operator {
        LIKE("LIKE"), //
        NOT_LIKE("NOT LIKE");

        private final String textRepresentation;

        PrestoLikeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static PrestoLikeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="), NOT_EQUALS("<>"), NOT_EQUALS_ALT("!="), IS_DISTINCT_FROM("IS DISTINCT FROM"),
        IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM"), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"),
        SMALLER_EQUALS("<=");

        private final String textRepresentation;

        PrestoBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static BinaryOperatorNode.Operator getRandomStringOperator() {
            return Randomly.fromOptions(EQUALS, NOT_EQUALS, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);
        }

        public static BinaryOperatorNode.Operator getRandomForType(PrestoSchema.PrestoCompositeDataType type) {
            PrestoSchema.PrestoDataType dataType = type.getPrimitiveDataType();

            switch (dataType) {
            case BOOLEAN:
            case INT:
            case FLOAT:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return getRandom();
            case VARCHAR:
            case CHAR:
            case VARBINARY:
            case JSON:
            case ARRAY:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
                // return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT,
                // IS_DISTINCT_FROM,
                // IS_NOT_DISTINCT_FROM);
            default:
                return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);
            }
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%");

        private final String textRepresentation;

        PrestoBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoUnaryArithmeticOperator implements BinaryOperatorNode.Operator {
        MINUS("-");

        private final String textRepresentation;

        PrestoUnaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC
    }

    @Override
    public PrestoTypedExpressionGenerator setTablesAndColumns(AbstractTables<PrestoTable, PrestoColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public PrestoExpression generateBooleanExpression() {
        return generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN),
                randomly.getInteger(0, maxDepth));
    }

    @Override
    public PrestoSelect generateSelect() {
        return new PrestoSelect();
    }

    @Override
    public List<PrestoJoin> getRandomJoinClauses() {
        List<PrestoTableReference> tableList = tables.stream().map(t -> new PrestoTableReference(t))
                .collect(Collectors.toList());
        List<PrestoJoin> joins = PrestoJoin.getJoins(tableList, globalState);
        tables = tableList.stream().map(t -> t.getTable()).collect(Collectors.toList());
        return joins;
    }

    @Override
    public List<PrestoExpression> getTableRefs() {
        return tables.stream().map(t -> new PrestoTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public String generateOptimizedQueryString(PrestoSelect select, PrestoExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            PrestoFunctionNode<PrestoAggregateFunction> aggr = new PrestoFunctionNode<>(
                    List.of(new PrestoColumnReference(new PrestoColumn("*",
                            new PrestoCompositeDataType(PrestoDataType.INT, 0, 0), false, false))),
                    PrestoAggregateFunction.COUNT);
            select.setFetchColumns(List.of(aggr));

        } else {
            List<PrestoExpression> allColumns = columns.stream().map((c) -> new PrestoColumnReference(c))
                    .collect(Collectors.toList());
            select.setFetchColumns(allColumns);
            if (Randomly.getBooleanWithSmallProbability()) {
                select.setOrderByClauses(generateOrderBys());
            }
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(PrestoSelect select, PrestoExpression whereCondition) {
        PrestoExpression asText = new PrestoPostfixText(

                new PrestoCastFunction(
                        new PrestoPostfixText(whereCondition,
                                " IS NOT NULL AND " + PrestoToStringVisitor.asString(whereCondition)),
                        new PrestoCompositeDataType(PrestoDataType.INT, 8, 0)),
                "as count");

        select.setFetchColumns(List.of(asText));
        select.setWhereClause(null);
        return "SELECT SUM(count) FROM (" + PrestoToStringVisitor.asString(select) + ") as res";
    }

    @Override
    public List<PrestoExpression> generateFetchColumns(boolean shouldCreateDummy) {
        if (Randomly.getBoolean()) {
            return List.of(new PrestoColumnReference(new PrestoColumn("*", null, false, false)));
        }
        return Randomly.nonEmptySubset(columns).stream().map(c -> new PrestoColumnReference(c))
                .collect(Collectors.toList());
    }
}
