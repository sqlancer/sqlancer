package sqlancer.databend.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.databend.DatabendSchema.DatabendDataType;


public class DatabendUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<DatabendExpression> {

//    private final Node<DatabendExpression> expr;
//    private final DatabendUnaryPostfixOperator op;
    private boolean negate;

    public DatabendUnaryPostfixOperation(Node<DatabendExpression> expr, DatabendUnaryPostfixOperator op, boolean negate) {
        super(expr,op);
        setNegate(negate);
    }

    public enum DatabendUnaryPostfixOperator implements BinaryOperatorNode.Operator {
        IS_NULL("IS NULL"){
            @Override
            public DatabendDataType[] getInputDataTypes() {
                return DatabendDataType.values();
            }
        },
        IS_NOT_NULL("IS NOT NULL"){
            @Override
            public DatabendDataType[] getInputDataTypes() {
                return DatabendDataType.values();
            }
        };
        //IS

        private final String textRepresentations;

        DatabendUnaryPostfixOperator(String text) {
            this.textRepresentations = text;
        }

        public static DatabendUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentations;
        }

        public abstract DatabendDataType[] getInputDataTypes();

    }

    public boolean isNegated() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

//    @Override
    public Node<DatabendExpression> getExpression() {
        return getExpr();
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op.getTextRepresentation();
    }

//    @Override
//    public OperatorKind getOperatorKind() {
//        return OperatorKind.POSTFIX;
//    }
}
