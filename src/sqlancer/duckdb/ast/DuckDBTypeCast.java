package sqlancer.duckdb.ast;

import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;

public class DuckDBTypeCast implements DuckDBExpression {
    DuckDBExpression expr;
    DuckDBCompositeDataType type;

    public DuckDBTypeCast(DuckDBExpression e, DuckDBCompositeDataType t) {
        this.expr = e;
        this.type = t;
    }

    public DuckDBExpression getExpression() {
        return this.expr;
    }

    public DuckDBCompositeDataType getType() {
        return this.type;
    }
}
