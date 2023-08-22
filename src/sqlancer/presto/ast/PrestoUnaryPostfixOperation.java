package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

public class PrestoUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<PrestoExpression> {

    public PrestoUnaryPostfixOperation(Node<PrestoExpression> expr, PrestoUnaryPostfixOperator op) {
        super(expr, op);
    }

    public Node<PrestoExpression> getExpression() {
        return getExpr();
    }

    public enum PrestoUnaryPostfixOperator implements BinaryOperatorNode.Operator {
        IS_NULL("IS NULL") {
            @Override
            public PrestoSchema.PrestoDataType[] getInputDataTypes() {
                return PrestoSchema.PrestoDataType.values();
            }
        },
        IS_NOT_NULL("IS NOT NULL") {
            @Override
            public PrestoSchema.PrestoDataType[] getInputDataTypes() {
                return PrestoSchema.PrestoDataType.values();
            }
        };

        private final String textRepresentations;

        PrestoUnaryPostfixOperator(String text) {
            this.textRepresentations = text;
        }

        public static PrestoUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentations;
        }

        public abstract PrestoSchema.PrestoDataType[] getInputDataTypes();

    }

}
