package sqlancer.mysql.ast;

public class MySQLCastOperation implements MySQLExpression {

    private final MySQLExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
        }

    }

    public MySQLCastOperation(MySQLExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(type);
    }

}
