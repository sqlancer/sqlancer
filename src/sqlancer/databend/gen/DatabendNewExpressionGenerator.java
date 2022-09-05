package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.ast.*;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation.DatabendUnaryPostfixOperator;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation.DatabendUnaryPrefixOperator;
import sqlancer.databend.ast.DatabendBinaryLogicalOperation.DatabendBinaryLogicalOperator;
import sqlancer.databend.ast.DatabendBinaryComparisonOperation.DatabendBinaryComparisonOperator;
import sqlancer.databend.ast.DatabendBinaryArithmeticOperation.DatabendBinaryArithmeticOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabendNewExpressionGenerator extends
        TypedExpressionGenerator<Node<DatabendExpression>, DatabendColumn, DatabendDataType> {

    private final DatabendGlobalState globalState;
    private boolean allowAggregateFunctions;

    public DatabendNewExpressionGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    public Node<DatabendExpression> generateLeafNode(DatabendDataType dataType) {
        return generateConstant(dataType);
    }


    @Override
    protected Node<DatabendExpression> generateExpression(DatabendDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }

        switch (type) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INT:
                return generateIntExpression(depth);
            case FLOAT:
            case VARCHAR:
            case NULL:
                return generateConstant(type);
            default:
                throw new AssertionError();
        }
    }

    public List<Node<DatabendExpression>> generateExpressions(int nr, DatabendDataType type) {
        List<Node<DatabendExpression>> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(type));
        }
        return expressions;
    }

    private enum IntExpression{
        UNARY_OPERATION, BINARY_ARITHMETIC_OPERATION
    }

    private Node<DatabendExpression> generateIntExpression(int depth) {
        IntExpression intExpression = Randomly.fromOptions(IntExpression.values());
        switch (intExpression) {
            case UNARY_OPERATION:
                return new DatabendUnaryPrefixOperation(generateExpression(DatabendDataType.INT,depth+1),
                        Randomly.getBoolean()? DatabendUnaryPrefixOperator.UNARY_PLUS : DatabendUnaryPrefixOperator.UNARY_MINUS);
            case BINARY_ARITHMETIC_OPERATION:
                return new DatabendBinaryArithmeticOperation(generateExpression(DatabendDataType.INT,depth+1),
                        generateExpression(DatabendDataType.INT,depth+1),
                        Randomly.fromOptions(DatabendBinaryArithmeticOperator.values()));
            default:
                throw new AssertionError();
        }
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION;
//        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON,FUNCTION, CAST,;
    }

    Node<DatabendExpression> generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
            case POSTFIX_OPERATOR:
                getPostfix(depth + 1);
            case NOT:
                getNOT(depth + 1);
            case BETWEEN: //TODO (NULL BETWEEN NULL AND NULL) 返回的是 NULL 需要注意
                return getBetween(depth + 1);
            case IN_OPERATION:
                return getIn(depth + 1);
            case BINARY_LOGICAL_OPERATOR:
                return getBinaryLogical(depth + 1,DatabendDataType.BOOLEAN);
            case BINARY_COMPARISON:
                return getComparison(depth + 1);
            case LIKE:
                return getLike(depth + 1,DatabendDataType.VARCHAR);
            default:
                throw new AssertionError();
        }

    }

    Node<DatabendExpression> getPostfix(int depth) {
        DatabendUnaryPostfixOperator randomOp = DatabendUnaryPostfixOperator.getRandom();
        return new DatabendUnaryPostfixOperation(
                generateExpression(Randomly.fromOptions(randomOp.getInputDataTypes()), depth),
                randomOp,Randomly.getBoolean());
    }

    Node<DatabendExpression> getNOT(int depth) {
        DatabendUnaryPrefixOperator op = DatabendUnaryPrefixOperator.NOT;
        return new DatabendUnaryPrefixOperation(
                generateExpression(Randomly.fromOptions(op.getInputDataTypes()), depth),
                op,Randomly.getBoolean());
    }

    Node<DatabendExpression> getBetween(int depth){
        //跳过boolean
        DatabendDataType dataType = Randomly.fromList(Arrays.asList(DatabendDataType.values()).stream()
                .filter(t -> t != DatabendDataType.BOOLEAN).collect(Collectors.toList()));

        return new NewBetweenOperatorNode<DatabendExpression>(generateExpression(dataType,depth),
                generateExpression(dataType,depth), generateExpression(dataType,depth),
                Randomly.getBoolean());
    }

    Node<DatabendExpression> getIn(int depth) {
        DatabendDataType dataType = Randomly.fromOptions(DatabendDataType.values());
        Node<DatabendExpression> leftExpr = generateExpression(dataType,depth);
        List<Node<DatabendExpression>> rightExprs = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        for(int i = 0; i < nr; i++) {
            rightExprs.add(generateExpression(dataType,depth));
        }
        return new NewInOperatorNode<DatabendExpression>(leftExpr,rightExprs, Randomly.getBoolean());
    }

    Node<DatabendExpression> getBinaryLogical(int depth, DatabendDataType dataType){
        Node<DatabendExpression> expr = generateExpression(dataType,depth);
        int nr = Randomly.smallNumber() + 1;
        for (int i = 0; i < nr; i++) {
            expr = new DatabendBinaryLogicalOperation(expr,
                    generateExpression(DatabendDataType.BOOLEAN, depth),
                    DatabendBinaryLogicalOperator.getRandom());
        }
        return expr;
    }

    Node<DatabendExpression> getComparison(int depth) {
        //跳过boolean
        DatabendDataType dataType = Randomly.fromList(Arrays.asList(DatabendDataType.values()).stream()
                .filter(t -> t != DatabendDataType.BOOLEAN).collect(Collectors.toList()));
        Node<DatabendExpression> leftExpr = generateExpression(dataType,depth);
        Node<DatabendExpression> rightExpr = generateExpression(dataType,depth);
        DatabendBinaryComparisonOperation op = new DatabendBinaryComparisonOperation(leftExpr,rightExpr,
                Randomly.fromOptions(DatabendBinaryComparisonOperator.values()));
        return op;
    }

    Node<DatabendExpression> getLike(int depth, DatabendDataType dataType) {
        return new DatabendLikeOperation(generateExpression(dataType,depth)
                ,generateExpression(dataType,depth), DatabendLikeOperation.DatabendLikeOperator.LIKE_OPERATOR);
    }

    @Override
    public Node<DatabendExpression> generatePredicate() {
        return generateExpression(DatabendDataType.BOOLEAN);
    }

    @Override
    public Node<DatabendExpression> negatePredicate(Node<DatabendExpression> predicate) {
        return new DatabendUnaryPrefixOperation(predicate,DatabendUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<DatabendExpression> isNull(Node<DatabendExpression> predicate) {
        return new DatabendUnaryPostfixOperation(predicate,DatabendUnaryPostfixOperator.IS_NULL);
    }

    public Node<DatabendExpression> generateConstant(DatabendDataType type,boolean isNullable) {
        if(isNullable && Randomly.getBooleanWithSmallProbability()) {
            createConstant(DatabendDataType.NULL);
        }
        return createConstant(type);
    }

    @Override
    public Node<DatabendExpression> generateConstant(DatabendDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DatabendConstant.createNullConstant();
        }
        return createConstant(type);
    }

    public Node<DatabendExpression> createConstant(DatabendDataType type) {
        Randomly r = globalState.getRandomly();
        switch (type) {
            case INT:
                //TODO 已支持数值型string转化，待添加
                return DatabendConstant.createIntConstant(r.getInteger());
            case BOOLEAN:
                //TODO 已支持boolean型string转化，待添加
                return DatabendConstant.createBooleanConstant(Randomly.getBoolean());
            case FLOAT:
                return DatabendConstant.createFloatConstant((float) r.getDouble());
            case VARCHAR:
                return DatabendConstant.createStringConstant(r.getString());
            case NULL:
                return DatabendConstant.createNullConstant();
            default:
                throw new AssertionError(type);
        }
    }

    @Override
    protected Node<DatabendExpression> generateColumn(DatabendDataType type) {
        return null;
    }

    @Override
    protected DatabendDataType getRandomType() {
        return Randomly.fromOptions(DatabendDataType.values());
    }

    @Override
    protected boolean canGenerateColumnOfType(DatabendDataType type) {
        return false;
    }

    public enum DatabendAggregateFunction {
        MAX(1),
        MIN(1),
        AVG(1,DatabendDataType.INT,DatabendDataType.FLOAT),
        COUNT(1),
        SUM(1,DatabendDataType.INT,DatabendDataType.FLOAT),
        STDDEV_POP(1),
        COVAR_POP(1), COVAR_SAMP(2);
        //, STRING_AGG(1), STDDEV_SAMP(1),VAR_SAMP(1), VAR_POP(1)

        private int nrArgs;
        private DatabendDataType[] dataTypes;

        DatabendAggregateFunction(int nrArgs, DatabendDataType ...dataTypes) {
            this.nrArgs = nrArgs;
            this.dataTypes = dataTypes;
        }

        public static DatabendAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public DatabendDataType getRandomType() {
            if(dataTypes.length == 0) {
                return Randomly.fromOptions(DatabendDataType.values());
            } else {
                return Randomly.fromOptions(dataTypes);
            }
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public NewFunctionNode<DatabendExpression, DatabendAggregateFunction> generateArgsForAggregate(
            DatabendAggregateFunction aggregateFunction) {
        return new NewFunctionNode<DatabendExpression, DatabendAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs(),aggregateFunction.getRandomType()),
                aggregateFunction);
    }

    public Node<DatabendExpression> generateAggregate() {
        DatabendAggregateFunction aggrFunc = DatabendAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    public Node<DatabendExpression> generateHavingClause() {
        this.allowAggregateFunctions = true;
        Node<DatabendExpression> expression = generateExpression(DatabendDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

}
