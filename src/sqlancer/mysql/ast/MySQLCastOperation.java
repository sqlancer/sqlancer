package sqlancer.mysql.ast;

public class MySQLCastOperation implements MySQLExpression {

    private final MySQLExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED,
        // CHAR, FLOAT, DOUBLE and DECIMAL are used only by the EET oracle's type-pinning casts and are never
        // evaluated, so MySQLConstant.castAs does not support them; they must not be returned by getRandom().
        CHAR, FLOAT, DOUBLE, DECIMAL;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.SIGNED, CastType.UNSIGNED);
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
