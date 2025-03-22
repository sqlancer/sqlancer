package sqlancer.tidb.ast;

import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBTableAndColumnReference implements TiDBExpression {
    private final TiDBTable table;

    public TiDBTableAndColumnReference(TiDBTable table) {
        this.table = table;
    }

    public TiDBTable getTable() {
        return table;
    }
}
