package sqlancer.hsqldb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<HSQLDBExpression> {

    // private final HSQLDBUnaryPrefixOperation.HSQLDBUnaryPrefixOperator operation;
    // private final Node<HSQLDBExpression> expression;

    public HSQLDBUnaryPrefixOperation(HSQLDBUnaryPrefixOperator operation, Node<HSQLDBExpression> expression) {
        super(expression, operation);
        // this.operation = operation;
        // this.expression = expression;
    }

    @Override
    public String getOperatorRepresentation() {
        return null;
    }

    public enum HSQLDBUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("NOT", HSQLDBSchema.HSQLDBDataType.BOOLEAN, HSQLDBSchema.HSQLDBDataType.INTEGER) {
            @Override
            public HSQLDBSchema.HSQLDBDataType getExpressionType() {
                return HSQLDBSchema.HSQLDBDataType.BOOLEAN;
            }

            @Override
            protected HSQLDBConstant getExpectedValue(HSQLDBConstant expectedValue) {
                return null; // TODO
            }
        },

        UNARY_PLUS("+", HSQLDBSchema.HSQLDBDataType.INTEGER) {
            @Override
            public HSQLDBSchema.HSQLDBDataType getExpressionType() {
                return HSQLDBSchema.HSQLDBDataType.INTEGER;
            }

            @Override
            protected HSQLDBConstant getExpectedValue(HSQLDBConstant expectedValue) {
                return expectedValue;
            }
        },
        UNARY_MINUS("-", HSQLDBSchema.HSQLDBDataType.INTEGER) {
            @Override
            public HSQLDBSchema.HSQLDBDataType getExpressionType() {
                return HSQLDBSchema.HSQLDBDataType.INTEGER;
            }

            @Override
            protected HSQLDBConstant getExpectedValue(HSQLDBConstant expectedValue) {
                return null;
            }
        };

        private String textRepresentation;
        private HSQLDBSchema.HSQLDBDataType[] dataTypes;

        HSQLDBUnaryPrefixOperator(String textRepresentation, HSQLDBSchema.HSQLDBDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract HSQLDBSchema.HSQLDBDataType getExpressionType();

        public HSQLDBSchema.HSQLDBDataType getRandomInputDataTypes() {
            return Randomly.fromOptions(dataTypes);
        }

        protected abstract HSQLDBConstant getExpectedValue(HSQLDBConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return this.textRepresentation;
        }
    }

}
