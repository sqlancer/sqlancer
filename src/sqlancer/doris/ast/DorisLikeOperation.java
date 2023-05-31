package sqlancer.doris.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.visitor.DorisExprToNode;

public class DorisLikeOperation extends NewBinaryOperatorNode<DorisExpression> implements DorisExpression {

    public DorisLikeOperation(DorisExpression left, DorisExpression right, DorisLikeOperator op) {
        super(DorisExprToNode.cast(left), DorisExprToNode.cast(right), op);
    }

    @Override
    public DorisDataType getExpectedType() {
        return DorisDataType.BOOLEAN;
    }

    public DorisExpression getLeftExpr() {
        return (DorisExpression) super.getLeft();
    }

    public DorisExpression getRightExpr() {
        return (DorisExpression) super.getRight();
    }

    public DorisLikeOperator getOp() {
        return (DorisLikeOperator) op;
    }

    @Override
    public DorisConstant getExpectedValue() {
        DorisConstant leftVal = getLeftExpr().getExpectedValue();
        DorisConstant rightVal = getRightExpr().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        return getOp().apply(leftVal, rightVal);
    }

    public enum DorisLikeOperator implements BinaryOperatorNode.Operator {
        LIKE_OPERATOR("LIKE", "like") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (left == null || right == null) {
                    return null;
                }
                if (left.isNull() || right.isNull()) {
                    return DorisConstant.createNullConstant();
                }
                boolean result = LikeImplementationHelper.match(left.asString(), right.asString(), 0, 0, true);
                return DorisConstant.createBooleanConstant(result);
            }
        },
        NOT_LIKE("NOT LIKE", "not like") {
            @Override
            public DorisConstant apply(DorisConstant left, DorisConstant right) {
                if (left == null || right == null) {
                    return null;
                }
                if (left.isNull() || right.isNull()) {
                    return DorisConstant.createNullConstant();
                }
                boolean result = LikeImplementationHelper.match(left.asString(), right.asString(), 0, 0, true);
                return DorisConstant.createBooleanConstant(!result);
            }
        };

        private final String[] textRepresentations;

        DorisLikeOperator(String... text) {
            textRepresentations = text.clone();
        }

        public abstract DorisConstant apply(DorisConstant left, DorisConstant right);

        @Override
        public String getTextRepresentation() {
            return " " + Randomly.fromOptions(textRepresentations) + " ";
        }
    }

}
