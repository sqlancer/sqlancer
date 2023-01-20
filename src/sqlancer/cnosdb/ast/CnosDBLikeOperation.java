package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryNode;

public class CnosDBLikeOperation extends BinaryNode<CnosDBExpression> implements CnosDBExpression {

    public CnosDBLikeOperation(CnosDBExpression left, CnosDBExpression right) {
        super(left, right);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}
