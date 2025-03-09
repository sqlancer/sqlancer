package sqlancer.sqlite3.ast;

import sqlancer.common.ast.newast.Join;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3Join
        implements SQLite3Expression, Join<SQLite3Expression, SQLite3Schema.SQLite3Table, SQLite3Schema.SQLite3Column> {

    public enum JoinType {
        INNER, CROSS, OUTER, NATURAL, RIGHT, FULL;
    }

    private final SQLite3Schema.SQLite3Table table;
    private SQLite3Expression onClause;
    private JoinType type;

    public SQLite3Join(SQLite3Join other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public SQLite3Join(SQLite3Schema.SQLite3Table table, SQLite3Expression onClause, JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public SQLite3Join(SQLite3Schema.SQLite3Table table, JoinType type) {
        this.table = table;
        if (type != JoinType.NATURAL) {
            throw new AssertionError();
        }
        this.onClause = null;
        this.type = type;
    }

    public SQLite3Schema.SQLite3Table getTable() {
        return table;
    }

    public SQLite3Expression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public void setOnClause(SQLite3Expression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }
}
