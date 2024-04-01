package sqlancer.sqlite3.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3NoRECOracle implements TestOracle<SQLite3GlobalState> {

    NoRECOracle<SQLite3Select, Join, SQLite3Expression, SQLite3Schema, SQLite3Table, SQLite3Column, SQLite3GlobalState> oracle;

    public SQLite3NoRECOracle(SQLite3GlobalState globalState) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                .with(SQLite3Errors.getMatchQueryErrors()).with(SQLite3Errors.getQueryErrors())
                .with("misuse of aggregate", "misuse of window function",
                        "second argument to nth_value must be a positive integer", "no such table", "no query solution",
                        "unable to use function MATCH in the requested context")
                .build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<SQLite3GlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
