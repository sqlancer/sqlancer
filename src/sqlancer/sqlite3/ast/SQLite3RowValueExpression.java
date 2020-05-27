package sqlancer.sqlite3.ast;

import java.util.List;

import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3RowValueExpression extends SQLite3Expression {

    private final List<SQLite3Expression> expressions;

    public SQLite3RowValueExpression(List<SQLite3Expression> expressions) {
        this.expressions = expressions;
    }

    public List<SQLite3Expression> getExpressions() {
        return expressions;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        for (SQLite3Expression expr : expressions) {
            SQLite3CollateSequence collate = expr.getExplicitCollateSequence();
            if (collate != null) {
                return collate;
            }
        }
        return null;
    }

}
