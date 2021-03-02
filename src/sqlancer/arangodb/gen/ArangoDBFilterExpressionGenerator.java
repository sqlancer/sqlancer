package sqlancer.arangodb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.arangodb.ast.ArangoDBConstant;
import sqlancer.arangodb.ast.ArangoDBExpression;
import sqlancer.arangodb.ast.ArangoDBUnsupportedPredicate;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;

public class ArangoDBFilterExpressionGenerator
        extends UntypedExpressionGenerator<Node<ArangoDBExpression>, ArangoDBSchema.ArangoDBColumn> {

    private final ArangoDBProvider.ArangoDBGlobalState globalState;
    private int numberOfComputedVariables;

    private enum Expression {
        BINARY_LOGICAL, UNARY_PREFIX, BINARY_COMPARISON
    }

    public ArangoDBFilterExpressionGenerator(ArangoDBProvider.ArangoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public void setNumberOfComputedVariables(int numberOfComputedVariables) {
        this.numberOfComputedVariables = numberOfComputedVariables;
    }

    @Override
    public Node<ArangoDBExpression> generateConstant() {
        ArangoDBSchema.ArangoDBDataType dataType = ArangoDBSchema.ArangoDBDataType.getRandom();
        switch (dataType) {
        case INTEGER:
            return ArangoDBConstant.createIntegerConstant((int) globalState.getRandomly().getInteger());
        case BOOLEAN:
            return ArangoDBConstant.createBooleanConstant(Randomly.getBoolean());
        case DOUBLE:
            return ArangoDBConstant.createDoubleConstant(globalState.getRandomly().getDouble());
        case STRING:
            return ArangoDBConstant.createStringConstant(globalState.getRandomly().getString());
        default:
            throw new AssertionError(dataType);
        }
    }

    @Override
    protected Node<ArangoDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        Expression expression = Randomly.fromList(possibleOptions);
        switch (expression) {
        case BINARY_COMPARISON:
            BinaryOperatorNode.Operator op = ArangoDBBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<>(generateExpression(depth + 1),
                    ArangoDBUnaryPrefixOperator.getRandom());
        case BINARY_LOGICAL:
            op = ArangoDBBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<>(generateExpression(depth + 1), generateExpression(depth + 1), op);
        default:
            throw new AssertionError(expression);
        }
    }

    @Override
    protected Node<ArangoDBExpression> generateColumn() {
        ArangoDBSchema.ArangoDBTable dummy = new ArangoDBSchema.ArangoDBTable("", new ArrayList<>(), false);
        if (Randomly.getBoolean() || numberOfComputedVariables == 0) {
            ArangoDBSchema.ArangoDBColumn column = Randomly.fromList(columns);
            return new ColumnReferenceNode<>(column);
        } else {
            int maxNumber = globalState.getRandomly().getInteger(0, numberOfComputedVariables);
            ArangoDBSchema.ArangoDBColumn column = new ArangoDBSchema.ArangoDBColumn("c" + maxNumber,
                    ArangoDBSchema.ArangoDBDataType.INTEGER, false, false);
            column.setTable(dummy);
            return new ColumnReferenceNode<>(column);
        }
    }

    @Override
    public Node<ArangoDBExpression> negatePredicate(Node<ArangoDBExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, ArangoDBUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<ArangoDBExpression> isNull(Node<ArangoDBExpression> expr) {
        return new ArangoDBUnsupportedPredicate<>();
    }

    public enum ArangoDBBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("=="), NOT_EQUALS("!="), LESS_THAN("<"), LESS_OR_EQUAL("<="), GREATER_THAN(">"), GREATER_OR_EQUAL(">=");

        private final String representation;

        ArangoDBBinaryComparisonOperator(String representation) {
            this.representation = representation;
        }

        @Override
        public String getTextRepresentation() {
            return representation;
        }

        public static ArangoDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum ArangoDBUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("!");

        private final String representation;

        ArangoDBUnaryPrefixOperator(String representation) {
            this.representation = representation;
        }

        @Override
        public String getTextRepresentation() {
            return representation;
        }

        public static ArangoDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum ArangoDBBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

}
