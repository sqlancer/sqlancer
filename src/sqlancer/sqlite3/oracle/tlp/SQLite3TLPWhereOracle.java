package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3TLPWhereOracle implements TestOracle<SQLite3GlobalState> {

    private final TLPWhereOracle<SQLite3Select, SQLite3Expression.Join, SQLite3Expression, SQLite3Schema, SQLite3Table, SQLite3Column, SQLite3GlobalState> oracle;

    public SQLite3TLPWhereOracle(SQLite3GlobalState state) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                .build();
        this.oracle = new TLPWhereOracle<>(state, gen, expectedErrors);
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
