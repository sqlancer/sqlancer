package sqlancer.sqlite3;

import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Cast;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;

public final class SQLite3CollateHelper {

    private SQLite3CollateHelper() {
    }

    public static boolean shouldGetSubexpressionAffinity(SQLite3Expression expression) {
        return expression instanceof SQLite3UnaryOperation
                && ((SQLite3UnaryOperation) expression).getOperation() == UnaryOperator.PLUS
                || expression instanceof Cast || expression instanceof SQLite3ColumnName;
    }

}
