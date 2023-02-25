package sqlancer.hsqldb.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<HSQLDBExpression> {

    public HSQLDBUnaryPostfixOperation(Node<HSQLDBExpression> expr, HSQLDBUnaryPostfixOperator op) {
        super(expr, op);
    }

    public enum HSQLDBUnaryPostfixOperator implements BinaryOperatorNode.Operator {
        IS_NULL("IS NULL") {
            @Override
            public HSQLDBSchema.HSQLDBDataType[] getInputDataTypes() {
                return HSQLDBSchema.HSQLDBDataType.values();
            }
        },
        IS_NOT_NULL("IS NOT NULL") {
            @Override
            public HSQLDBSchema.HSQLDBDataType[] getInputDataTypes() {
                return HSQLDBSchema.HSQLDBDataType.values();
            }
        };

        private final String textRepresentations;

        HSQLDBUnaryPostfixOperator(String text) {
            this.textRepresentations = text;
        }

        public static HSQLDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentations;
        }

        public abstract HSQLDBSchema.HSQLDBDataType[] getInputDataTypes();

    }

    public Node<HSQLDBExpression> getExpression() {
        return getExpr();
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op.getTextRepresentation();
    }

}
