package sqlancer.sqlite3.ast;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3Join extends JoinBase<SQLite3Expression>
        implements SQLite3Expression, Join<SQLite3Expression, SQLite3Schema.SQLite3Table, SQLite3Schema.SQLite3Column> {

    private final SQLite3Table table;

    public SQLite3Join(SQLite3Join other) {

        super(null, other.onClause, other.type);
        this.table = other.table;
    }

    public SQLite3Join(SQLite3Table table, SQLite3Expression onClause, JoinType type) {

        super(null, onClause, type);
        this.table = table;
    }

    public SQLite3Join(SQLite3Table table, JoinType type) {
        super(null, null, type);
        this.table = table;
        if (type != JoinType.NATURAL) {
            throw new AssertionError();
        }
    }

    public SQLite3Table getTable() {
        return table;
    }

    public SQLite3Expression getOnClause() {
        return onClause;
    }


    public JoinType getType() {
        return type;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
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
