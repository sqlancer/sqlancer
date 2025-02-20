package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.SQLite3CollateHelper;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3ExpressionCast implements SQLite3Expression {

    private final SQLite3TypeLiteral type;
    private final SQLite3Expression expression;

    public SQLite3ExpressionCast(SQLite3TypeLiteral typeofExpr, SQLite3Expression expression) {
        this.type = typeofExpr;
        this.expression = expression;
    }

    public SQLite3Expression getExpression() {
        return expression;
    }

    public SQLite3TypeLiteral getType() {
        return type;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        if (expression.getExpectedValue() == null) {
            return null;
        } else {
            return type.type.apply(expression.getExpectedValue());
        }
    }

    /**
     * An expression of the form "CAST(expr AS type)" has an affinity that is the same as a column with a declared type
     * of "type".
     */
    @Override
    public SQLite3TypeAffinity getAffinity() {
        switch (type.type) {
        case BLOB:
            return SQLite3TypeAffinity.BLOB;
        case INTEGER:
            return SQLite3TypeAffinity.INTEGER;
        case NUMERIC:
            return SQLite3TypeAffinity.NUMERIC;
        case REAL:
            return SQLite3TypeAffinity.REAL;
        case TEXT:
            return SQLite3TypeAffinity.TEXT;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return expression.getExplicitCollateSequence();
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getImplicitCollateSequence() {
        if (SQLite3CollateHelper.shouldGetSubexpressionAffinity(expression)) {
            return expression.getImplicitCollateSequence();
        } else {
            return null;
        }
    }

}
