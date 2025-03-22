package sqlancer.cockroachdb.ast;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBTableAndColumnReference  implements CockroachDBExpression {
    private final CockroachDBTable table;

    public CockroachDBTableAndColumnReference(CockroachDBTable table) {
        this.table = table;
    }

    public CockroachDBTable getTable() {
        return table;
    }
}
