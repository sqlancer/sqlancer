package sqlancer.presto.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.ast.PrestoAggregateFunction;
import sqlancer.presto.ast.PrestoAtTimeZoneOperator;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoColumnReference;
import sqlancer.presto.ast.PrestoConstant;
import sqlancer.presto.ast.PrestoDefaultFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoMultiValuedComparison;
import sqlancer.presto.ast.PrestoMultiValuedComparisonOperator;
import sqlancer.presto.ast.PrestoMultiValuedComparisonType;
import sqlancer.presto.ast.PrestoQuantifiedComparison;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.ast.PrestoUnaryPostfixOperation;
import sqlancer.presto.ast.PrestoUnaryPrefixOperation;

public final class PrestoTypedExpressionGenerator extends
        TypedExpressionGenerator<Node<PrestoExpression>, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> {

    private final Randomly randomly;
    private final PrestoGlobalState globalState;
    private final int maxDepth;

    public PrestoTypedExpressionGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.randomly = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    @Override
    public Node<PrestoExpression> generatePredicate() {
        return generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN),
                randomly.getInteger(0, maxDepth));
    }

    @Override
    public Node<PrestoExpression> negatePredicate(Node<PrestoExpression> predicate) {
        return new PrestoUnaryPrefixOperation(PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT, predicate);
    }

    @Override
    public Node<PrestoExpression> isNull(Node<PrestoExpression> expr) {
        return new PrestoUnaryPostfixOperation(expr, PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public Node<PrestoExpression> generateConstant(PrestoSchema.PrestoCompositeDataType type) {
        if (Objects.requireNonNull(type.getPrimitiveDataType()) == PrestoSchema.PrestoDataType.ARRAY) {
            return PrestoConstant.createArrayConstant(type);
            // case MAP:
            // return PrestoConstant.createMapConstant(type);
        }
        return PrestoConstant.generateConstant(type, false);
    }

    public Node<PrestoExpression> generateInsertConstant(PrestoSchema.PrestoCompositeDataType type) {
        if (Objects.requireNonNull(type.getPrimitiveDataType()) == PrestoSchema.PrestoDataType.ARRAY) {
            return PrestoConstant.createArrayConstant(type);
            // case MAP:
            // return PrestoConstant.createMapConstant(type);
        }
        return PrestoConstant.generateConstant(type, true);
    }

    @Override
    public Node<PrestoExpression> generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
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

    private Node<PrestoExpression> generateJsonExpression(PrestoSchema.PrestoCompositeDataType type) {
        return generateLeafNode(type);
    }

    private Node<PrestoExpression> generateCast(PrestoSchema.PrestoCompositeDataType type, int depth) {
        // check can cast
        Node<PrestoExpression> expressionNode = generateExpression(getRandomType(), depth + 1);
        return new PrestoCastFunction(expressionNode, type);
    }

    @SuppressWarnings("unused")
    private Node<PrestoExpression> generateTry(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (type.getPrimitiveDataType().isNumeric() && Randomly.getBooleanWithRatherLowProbability()) {
            Node<PrestoExpression> expression = generateExpression(type);
            return new NewFunctionNode<>(List.of(expression), "try");
        }

        List<PrestoDefaultFunction> applicableFunctions = PrestoDefaultFunction.getFunctionsCompatibleWith(type);
        if (Randomly.getBooleanWithRatherLowProbability() && !applicableFunctions.isEmpty()) {
            PrestoDefaultFunction function = Randomly.fromList(applicableFunctions);
            Node<PrestoExpression> expression = generateFunction(type, depth, function);
            return new NewFunctionNode<>(List.of(expression), "try");
        }
        return new NewFunctionNode<>(List.of(generateCast(type, depth)), "try");
    }

    private NewCaseOperatorNode<PrestoExpression> getCase(PrestoSchema.PrestoCompositeDataType type, int depth) {
        List<Node<PrestoExpression>> conditions = new ArrayList<>();
        List<Node<PrestoExpression>> cases = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            conditions.add(generateExpression(type, depth + 1));
            cases.add(generateExpression(type, depth + 1));
        }
        Node<PrestoExpression> elseExpr = null;
        if (Randomly.getBoolean()) {
            elseExpr = generateExpression(type, depth + 1);
        }
        Node<PrestoExpression> expression = generateExpression(type);
        return new NewCaseOperatorNode<>(expression, conditions, cases, elseExpr);
    }

    private Node<PrestoExpression> generateFunction(PrestoSchema.PrestoCompositeDataType returnType, int depth,
            PrestoDefaultFunction function) {

        PrestoSchema.PrestoDataType[] argumentTypes = function.getArgumentTypes(returnType);
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

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
                Node<PrestoExpression> expression = generateExpression(dataType, depth + 1);
                arguments.add(expression);
            }
        }
        return new NewFunctionNode<>(arguments, function);
    }

    private Node<PrestoExpression> generateStringExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }
        return getStringOperation(depth);
    }

    private NewBinaryOperatorNode<PrestoExpression> getStringOperation(int depth) {
        StringExpression exprType = Randomly.fromOptions(StringExpression.values());
        if (Objects.requireNonNull(exprType) == StringExpression.CONCAT) {
            Node<PrestoExpression> left = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
            Node<PrestoExpression> right = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
            PrestBinaryStringOperator operator = PrestBinaryStringOperator.CONCAT;
            return new NewBinaryOperatorNode<>(left, right, operator);
        }
        throw new AssertionError(exprType);
    }

    private Node<PrestoExpression> generateBooleanExpression(int depth) {
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

    private Node<PrestoExpression> getMultiValuedComparison(int depth) {

        PrestoSchema.PrestoCompositeDataType type;
        do {
            type = PrestoSchema.PrestoCompositeDataType
                    .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getOrderableTypes()));
        } while (type.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY
                && !type.getElementType().getPrimitiveDataType().isOrderable());

        PrestoMultiValuedComparisonType comparisonType = PrestoMultiValuedComparisonType.getRandom();
        PrestoMultiValuedComparisonOperator comparisonOperator = PrestoMultiValuedComparisonOperator
                .getRandomForType(type);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
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
        List<Node<PrestoExpression>> rightList = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            rightList.add(generateConstant(type));
        }
        return new PrestoMultiValuedComparison(left, rightList, comparisonType, comparisonOperator);
    }

    private PrestoSelect generateSubquery(List<PrestoSchema.PrestoColumn> columns) {
        PrestoSelect select = new PrestoSelect();
        List<Node<PrestoExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<PrestoExpression, PrestoSchema.PrestoColumn>(c))
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        List<PrestoSchema.PrestoTable> tables = columns.stream().map(AbstractTableColumn::getTable)
                .collect(Collectors.toList());
        List<TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>(t)).distinct()
                .collect(Collectors.toList());
        List<Node<PrestoExpression>> tableNodeList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>(t))
                .collect(Collectors.toList());
        select.setFromList(tableNodeList);
        TypedExpressionGenerator<Node<PrestoExpression>, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> typedExpressionGenerator = new PrestoTypedExpressionGenerator(
                globalState).setColumns(columns);
        Node<PrestoExpression> predicate = typedExpressionGenerator.generatePredicate();
        select.setWhereClause(predicate);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(typedExpressionGenerator.generateOrderBys());
        }
        List<Node<PrestoExpression>> joins = PrestoJoin.getJoins(tableList, globalState);
        select.setJoinList(joins);
        return select;
    }

    private Node<PrestoExpression> generateNumericExpression(int depth) {
        PrestoSchema.PrestoDataType dataType = Randomly.fromList(PrestoSchema.PrestoDataType.getNumberTypes());
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
        if (Randomly.getBoolean()) {
            BinaryOperatorNode.Operator operator = PrestoBinaryArithmeticOperator.getRandom();
            Node<PrestoExpression> left = generateExpression(type, depth);
            Node<PrestoExpression> right = generateExpression(type, depth);
            return new NewBinaryOperatorNode<>(left, right, operator);
        } else {
            BinaryOperatorNode.Operator operator = PrestoUnaryArithmeticOperator.MINUS;
            Node<PrestoExpression> left = generateExpression(type, depth);
            return new NewUnaryPrefixOperatorNode<>(left, operator);
        }
    }

    private Node<PrestoExpression> generateTemporalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            Node<PrestoExpression> left = generateExpression(type, depth);
            Node<PrestoExpression> right = generateExpression(PrestoSchema.PrestoCompositeDataType
                    .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getIntervalTypes())), depth);
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            return new NewBinaryOperatorNode<>(left, right, operator);
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

    private Node<PrestoExpression> generateIntervalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            Node<PrestoExpression> left = generateExpression(type, depth);

            Node<PrestoExpression> right;
            if (Randomly.getBoolean()) {
                right = generateExpression(PrestoSchema.PrestoCompositeDataType
                        .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getTemporalTypes())), depth);
            } else {
                right = generateExpression(type, depth);
            }
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            if (Randomly.getBoolean()) {
                return new NewBinaryOperatorNode<>(left, right, operator);
            } else {
                return new NewBinaryOperatorNode<>(right, left, operator);
            }
        }
        return generateLeafNode(type);

        // functions

        // timestamp at time zone
    }

    private Node<PrestoExpression> getLike(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.VARCHAR);
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> pattern = generateExpression(type, depth + 1);
        if (Randomly.getBoolean()) {
            return new NewBinaryOperatorNode<>(expression, pattern, PrestoLikeOperator.getRandom());
        } else {
            String randomlyString = randomly.getString();
            String randomlyChar = randomly.getChar();
            Node<PrestoExpression> escape = new PrestoConstant.PrestoTextConstant(randomlyChar, 1);
            int index = randomlyString.indexOf(randomlyChar);
            while (index > -1) {
                String wildcard = Randomly.fromOptions("%", "_");
                randomlyString = randomlyString.substring(0, index + 1) + wildcard
                        + randomlyString.substring(index + 1);
                index = randomlyString.indexOf(randomlyChar, index + 1);
            }
            PrestoConstant.PrestoTextConstant patternString = new PrestoConstant.PrestoTextConstant(randomlyString);
            return new NewTernaryNode<>(expression, patternString, escape, "LIKE", "ESCAPE");
        }
    }

    private NewBinaryOperatorNode<PrestoExpression> getRegex(int depth) {
        Node<PrestoExpression> left = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
        Node<PrestoExpression> right = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.VARCHAR), depth + 1);
        return new NewBinaryOperatorNode<>(left, right, PrestoBinaryLogicalOperator.getRandom());
    }

    private NewBinaryOperatorNode<PrestoExpression> getBinaryLogical(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.BOOLEAN);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
        return new NewBinaryOperatorNode<>(left, right, operator);
    }

    private Node<PrestoExpression> getBetween(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getNumericTypes()));
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBetweenOperatorNode<>(expression, left, right, Randomly.getBoolean());
    }

    private Node<PrestoExpression> getInOperation(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType
                .fromDataType(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        List<Node<PrestoExpression>> inList = generateExpressions(type, Randomly.smallNumber() + 1, depth + 1);
        boolean isNegated = Randomly.getBoolean();
        return new NewInOperatorNode<>(left, inList, isNegated);
    }

    private Node<PrestoExpression> getAndOrChain(int depth) {
        Node<PrestoExpression> left = generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth + 1);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            Node<PrestoExpression> right = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth + 1);
            BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
            left = new NewBinaryOperatorNode<>(left, right, operator);
        }
        return left;
    }

    private Node<PrestoExpression> getBinaryComparison(int depth) {
        PrestoSchema.PrestoCompositeDataType type = getRandomType();
        BinaryOperatorNode.Operator op = PrestoBinaryComparisonOperator.getRandomForType(type);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBinaryOperatorNode<>(left, right, op);
    }

    private Node<PrestoExpression> generateNOT(int depth) {
        PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator operator = PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT;
        return new PrestoUnaryPrefixOperation(operator, generateExpression(
                PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), depth));
    }

    @Override
    protected Node<PrestoExpression> generateColumn(PrestoSchema.PrestoCompositeDataType type) {
        List<PrestoSchema.PrestoColumn> columnList = columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == type.getPrimitiveDataType())
                .collect(Collectors.toList());
        PrestoSchema.PrestoColumn column = Randomly.fromList(columnList);
        return new PrestoColumnReference(column);
    }

    @Override
    public Node<PrestoExpression> generateLeafNode(PrestoSchema.PrestoCompositeDataType type) {
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

    public Node<PrestoExpression> generateAggregate() {
        PrestoAggregateFunction aggregateFunction = PrestoAggregateFunction.getRandom();
        List<Node<PrestoExpression>> argsForAggregate = generateArgsForAggregate(aggregateFunction);
        return new NewFunctionNode<>(argsForAggregate, aggregateFunction);
    }

    public List<Node<PrestoExpression>> generateArgsForAggregate(PrestoAggregateFunction aggregateFunction) {
        PrestoSchema.PrestoCompositeDataType returnType;
        do {
            returnType = aggregateFunction.getCompositeReturnType();
        } while (!aggregateFunction.isCompatibleWithReturnType(returnType));
        return aggregateFunction.getArgumentsForReturnType(this, this.maxDepth - 1, returnType, false);
    }

    private Node<PrestoExpression> generateAggregate(PrestoSchema.PrestoCompositeDataType type) {
        PrestoAggregateFunction aggregateFunction = Randomly
                .fromList(PrestoAggregateFunction.getFunctionsCompatibleWith(type));
        List<Node<PrestoExpression>> argsForAggregate = generateArgsForAggregate(type, aggregateFunction);
        return new NewFunctionNode<>(argsForAggregate, aggregateFunction);
    }

    public List<Node<PrestoExpression>> generateArgsForAggregate(PrestoSchema.PrestoCompositeDataType type,
            PrestoAggregateFunction aggregateFunction) {
        List<PrestoSchema.PrestoDataType> returnTypes = aggregateFunction.getReturnTypes(type.getPrimitiveDataType());
        List<Node<PrestoExpression>> arguments = new ArrayList<>();
        allowAggregates = false; //
        for (PrestoSchema.PrestoDataType argumentType : returnTypes) {
            arguments.add(generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(argumentType)));
        }
        // return new NewFunctionNode<>(arguments, aggregateFunction);
        return arguments;
    }

    @Override
    public List<Node<PrestoExpression>> generateOrderBys() {
        List<Node<PrestoExpression>> expressions = new ArrayList<>();
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

    public Node<PrestoExpression> generateHavingClause() {
        allowAggregates = true;
        Node<PrestoExpression> expr = generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
        allowAggregates = false;
        return expr;
    }

    public Node<PrestoExpression> generateExpressionWithColumns(List<PrestoSchema.PrestoColumn> columns,
            int remainingDepth) {
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
        return new NewBinaryOperatorNode<>(generateExpression(column.getType(), remainingDepth - 1),
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
                // return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT, IS_DISTINCT_FROM,
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

}
