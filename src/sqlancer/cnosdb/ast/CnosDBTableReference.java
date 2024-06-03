package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;

public class CnosDBTableReference implements CnosDBExpression {
    private final CnosDBTable table;

    public CnosDBTableReference(CnosDBTable table) {
        this.table = table;
    }
    
    public CnosDBTable getTable() {
        return table;
    }
}
