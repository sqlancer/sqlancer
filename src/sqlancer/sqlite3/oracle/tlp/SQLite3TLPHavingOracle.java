package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPHavingOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3TLPHavingOracle implements TestOracle<SQLite3GlobalState> {

    private final TLPHavingOracle<Join, SQLite3Expression, SQLite3Schema, SQLite3Table, SQLite3Column, SQLite3GlobalState> oracle;

    public SQLite3TLPHavingOracle(SQLite3GlobalState state) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                .with(SQLite3Errors.getExpectedExpressionErrors().toArray(new String[0])).with("no such column", // FIXME
                                                                                                                 // why?
                        "ON clause references tables to its right")
                .build();
        oracle = new TLPHavingOracle<>(state, gen, expectedErrors, false, true);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
