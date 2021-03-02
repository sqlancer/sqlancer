package sqlancer.arangodb.gen;

import sqlancer.Randomly;
import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.arangodb.ast.ArangoDBConstant;
import sqlancer.arangodb.ast.ArangoDBExpression;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;

public class ArangoDBComputedExpressionGenerator
        extends UntypedExpressionGenerator<Node<ArangoDBExpression>, ArangoDBSchema.ArangoDBColumn> {
    private final ArangoDBProvider.ArangoDBGlobalState globalState;

    public ArangoDBComputedExpressionGenerator(ArangoDBProvider.ArangoDBGlobalState globalState) {
        this.globalState = globalState;
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

    public enum ComputedFunction {
        ADD(2, "+"), MINUS(2, "-"), MULTIPLY(2, "*"), DIVISION(2, "/"), MODULUS(2, "%");

        private final int nrArgs;
        private final String operatorName;

        ComputedFunction(int nrArgs, String operatorName) {
            this.nrArgs = nrArgs;
            this.operatorName = operatorName;
        }

        public static ComputedFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

        public String getOperatorName() {
            return operatorName;
        }
    }

    @Override
    protected Node<ArangoDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        ComputedFunction function = ComputedFunction.getRandom();
        return new NewFunctionNode<>(generateExpressions(depth + 1, function.getNrArgs()), function);
    }

    @Override
    protected Node<ArangoDBExpression> generateColumn() {
        return new ColumnReferenceNode<>(Randomly.fromList(columns));
    }

    @Override
    public Node<ArangoDBExpression> negatePredicate(Node<ArangoDBExpression> predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node<ArangoDBExpression> isNull(Node<ArangoDBExpression> expr) {
        throw new UnsupportedOperationException();
    }
}
