package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3TableReference implements SQLite3Expression {

    private final String indexedBy;
    private final SQLite3Schema.SQLite3Table table;

    public SQLite3TableReference(String indexedBy, SQLite3Schema.SQLite3Table table) {
        this.indexedBy = indexedBy;
        this.table = table;
    }

    public SQLite3TableReference(SQLite3Schema.SQLite3Table table) {
        this.indexedBy = null;
        this.table = table;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    public SQLite3Schema.SQLite3Table getTable() {
        return table;
    }

    public String getIndexedBy() {
        return indexedBy;
    }

}