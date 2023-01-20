package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryNode;

public class CnosDBConcatOperation extends BinaryNode<CnosDBExpression> implements CnosDBExpression {

    public CnosDBConcatOperation(CnosDBExpression left, CnosDBExpression right) {
        super(left, right);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.STRING;
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
