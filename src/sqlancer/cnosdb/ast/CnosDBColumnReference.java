package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;

public class CnosDBColumnReference implements CnosDBExpression {
    private final CnosDBColumn c;

    public CnosDBColumnReference(CnosDBColumn c) {
        this.c = c;
    }

    public CnosDBColumn getColumn() {
        return c;
    }
}
