package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.SQLite3CollateHelper;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3ExpressionCast implements SQLite3Expression {

    private final TypeLiteral type;
    private final SQLite3Expression expression;

    public SQLite3ExpressionCast(TypeLiteral typeofExpr, SQLite3Expression expression) {
        this.type = typeofExpr;
        this.expression = expression;
    }

    public SQLite3Expression getExpression() {
        return expression;
    }

    public TypeLiteral getType() {
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
    public TypeAffinity getAffinity() {
        switch (type.type) {
        case BLOB:
            return TypeAffinity.BLOB;
        case INTEGER:
            return TypeAffinity.INTEGER;
        case NUMERIC:
            return TypeAffinity.NUMERIC;
        case REAL:
            return TypeAffinity.REAL;
        case TEXT:
            return TypeAffinity.TEXT;
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

class TypeLiteral {

    public final Type type;

    public enum Type {
        TEXT {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToText(cons);
            }
        },
        REAL {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToReal(cons);
            }
        },
        INTEGER {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToInt(cons);
            }
        },
        NUMERIC {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToNumeric(cons);
            }
        },
        BLOB {
            @Override
            public SQLite3Constant apply(SQLite3Constant cons) {
                return SQLite3Cast.castToBlob(cons);
            }
        };

        public abstract SQLite3Constant apply(SQLite3Constant cons);
    }

    public TypeLiteral(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}