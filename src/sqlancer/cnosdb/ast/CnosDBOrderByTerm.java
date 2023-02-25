package sqlancer.cnosdb.ast;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBOrderByTerm implements CnosDBExpression {

    private final CnosDBOrder order;
    private final CnosDBExpression expr;

    public CnosDBOrderByTerm(CnosDBExpression expr, CnosDBOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public CnosDBOrder getOrder() {
        return order;
    }

    public CnosDBExpression getExpr() {
        return expr;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return null;
    }

    public enum CnosDBOrder {
        ASC, DESC;

        public static CnosDBOrder getRandomOrder() {
            return Randomly.fromOptions(CnosDBOrder.values());
        }
    }

}
