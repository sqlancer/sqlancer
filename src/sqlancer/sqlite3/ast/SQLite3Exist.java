package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3Exist implements SQLite3Expression {

    private final SQLite3Expression select;

    public SQLite3Exist(SQLite3Expression select) {
        this.select = select;
    }

    public SQLite3Expression getExpression() {
        return select;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

}
