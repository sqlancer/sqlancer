package sqlancer.databend.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.databend.DatabendSchema.DatabendDataType;


public class DatabendUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<DatabendExpression> {

//    private final Node<DatabendExpression> expr;
//    private final DatabendUnaryPrefixOperator op;
    private boolean negate;

    public DatabendUnaryPrefixOperation(Node<DatabendExpression> expr, DatabendUnaryPrefixOperator op, boolean negate) {
        super(expr,op);
        setNegate(negate);
    }

    void setNegate(boolean negate){
        this.negate = negate;
    }

//    @Override
    public Node<DatabendExpression> getExpression() {
        return getExpr();
    }

    @Override
    public String getOperatorRepresentation() {
        return null;
    }

//    @Override
//    public OperatorKind getOperatorKind() {
//        return OperatorKind.PREFIX;
//    }



    public enum DatabendUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("NOT", DatabendDataType.BOOLEAN, DatabendDataType.INT) {
            @Override
            public DatabendDataType getExpressionType() {
                return DatabendDataType.BOOLEAN;
            }

            @Override
            protected DatabendConstant getExpectedValue(DatabendConstant expectedValue) {
                return null; // TODO
            }
        };


        private String textRepresentation;
        private DatabendDataType[] dataTypes;

        DatabendUnaryPrefixOperator(String textRepresentation, DatabendDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract DatabendDataType getExpressionType();

        public DatabendDataType[] getInputDataTypes(){
            return dataTypes;
        }

        protected abstract DatabendConstant getExpectedValue(DatabendConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return this.textRepresentation;
        }
    }

}
