package sqlancer.cnosdb.ast;

import java.util.List;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBInOperation implements CnosDBExpression {

    private final CnosDBExpression expr;
    private final List<CnosDBExpression> listElements;
    private final boolean isTrue;

    public CnosDBInOperation(CnosDBExpression expr, List<CnosDBExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public CnosDBExpression getExpr() {
        return expr;
    }

    public List<CnosDBExpression> getListElements() {
        return listElements;
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }
}
