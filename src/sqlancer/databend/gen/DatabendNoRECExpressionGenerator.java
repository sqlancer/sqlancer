package sqlancer.databend.gen;

import com.google.common.collect.RangeMap;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabendNoRECExpressionGenerator extends
        TypedExpressionGenerator<Node<DatabendExpression>, DatabendColumn, DatabendDataType> {

    private final DatabendGlobalState globalState;

    public DatabendNoRECExpressionGenerator(DatabendGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION;
//        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON,FUNCTION, CAST,;
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
            case FLOAT:
            case VARCHAR:
            case NULL:
                return generateConstant(type);
            default:
                throw new AssertionError();
        }
    }

    private Node<DatabendExpression> generateIntExpression(int depth) {

        return null;
    }

    Node<DatabendExpression> generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
            case POSTFIX_OPERATOR:
                getPostfix(depth + 1);
            case NOT:
                getNOT(depth + 1);
            case BETWEEN:
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
//        System.out.println("getPostfix:" + generateExpression(Randomly.fromOptions(randomOp.getInputDataTypes()), depth));
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
        return null;
    }

    @Override
    public Node<DatabendExpression> negatePredicate(Node<DatabendExpression> predicate) {
        return null;
    }

    @Override
    public Node<DatabendExpression> isNull(Node<DatabendExpression> expr) {
        return null;
    }

//    public Node<DatabendExpression> generateConstant(boolean isNullable) { //TODO 极小概率生成NULL值
//        DatabendDataType type;
//        do {
//            type = Randomly.fromOptions(DatabendDataType.values());
//        } while(!isNullable && type.equals(DatabendDataType.NULL)); //isNullable为假，同时获得NULL就重新获取
//
//        return generateConstant(type);
//    }

    @Override
    public Node<DatabendExpression> generateConstant(DatabendDataType type) {
        Randomly r = globalState.getRandomly();
        if (Randomly.getBooleanWithSmallProbability()) {
            return DatabendConstant.createNullConstant();
        }

        switch (type) {
            case INT:
                //不支持string转化故直接返回int constant
                return DatabendConstant.createIntConstant(r.getInteger());
            case BOOLEAN:
                //不支持string转化故直接返回boolean constant
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
}
