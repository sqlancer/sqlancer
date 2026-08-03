package sqlancer.mysql.ast;

import java.util.Objects;

public class MySQLCastOperation implements MySQLExpression {

    private final MySQLExpression expr;
    private final CastType type;

    /**
     * A MySQL {@code CAST} target type. The non-{@code DECIMAL} kinds are interned singletons; {@code DECIMAL} may
     * additionally carry an {@code (M, D)} precision/scale (via {@link #decimal}) so that a
     * {@code CAST(... AS DECIMAL(M, D))} can reproduce a column's exact type. This is relied on by the EET oracle's
     * type-pinning casts (see {@code MySQLEETTransformer}).
     */
    public static final class CastType {

        // CHAR, FLOAT, DOUBLE and DECIMAL are used only by the EET oracle's type-pinning casts and are never evaluated,
        // so MySQLConstant.castAs does not support them; they must not be returned by getRandom().
        public static final CastType SIGNED = new CastType(Kind.SIGNED);
        public static final CastType UNSIGNED = new CastType(Kind.UNSIGNED);
        public static final CastType CHAR = new CastType(Kind.CHAR);
        public static final CastType FLOAT = new CastType(Kind.FLOAT);
        public static final CastType DOUBLE = new CastType(Kind.DOUBLE);
        public static final CastType DECIMAL = new CastType(Kind.DECIMAL);

        private enum Kind {
            SIGNED, UNSIGNED, CHAR, FLOAT, DOUBLE, DECIMAL
        }

        private final Kind kind;
        private final Integer precision; // DECIMAL only, otherwise null
        private final Integer scale; // DECIMAL only, otherwise null

        private CastType(Kind kind) {
            this(kind, null, null);
        }

        private CastType(Kind kind, Integer precision, Integer scale) {
            this.kind = kind;
            this.precision = precision;
            this.scale = scale;
        }

        // A DECIMAL(precision, scale) cast target.
        public static CastType decimal(int precision, int scale) {
            return new CastType(Kind.DECIMAL, precision, scale);
        }

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.SIGNED, CastType.UNSIGNED);
        }

        public Integer getPrecision() {
            return precision;
        }

        public Integer getScale() {
            return scale;
        }

        @Override
        public String toString() {
            if (precision == null) {
                return kind.name();
            }
            return kind.name() + "(" + precision + ", " + scale + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CastType)) {
                return false;
            }
            CastType other = (CastType) obj;
            return kind == other.kind && Objects.equals(precision, other.precision)
                    && Objects.equals(scale, other.scale);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, precision, scale);
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
