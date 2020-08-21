package sqlancer.sqlite3.gen.ddl;

import java.sql.SQLException;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.oracle.SQLite3RandomQuerySynthesizer;
import sqlancer.sqlite3.schema.SQLite3Schema;

public final class SQLite3ViewGenerator {

    private SQLite3ViewGenerator() {
    }

    public static Query dropView(SQLite3GlobalState globalState) {
        SQLite3Schema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("DROP VIEW ");
        sb.append(s.getRandomViewOrBailout().getName());
        return new QueryAdapter(sb.toString(), true);
    }

    public static Query generate(SQLite3GlobalState globalState) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
        }
        sb.append(" VIEW ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS ");
        }
        sb.append(SQLite3Common.getFreeViewName(globalState.getSchema()));
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        errors.add("is circularly defined");
        errors.add("unsupported frame specification");
        errors.add("The database file is locked");
        int size = 1 + Randomly.smallNumber();
        columnNamesAs(sb, size);
        SQLite3Expression randomQuery = SQLite3RandomQuerySynthesizer.generate(globalState, size);
        sb.append(SQLite3Visitor.asString(randomQuery));
        return new QueryAdapter(sb.toString(), errors, true);

    }

    private static void columnNamesAs(StringBuilder sb, int size) {
        sb.append("(");
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(SQLite3Common.createColumnName(i));
        }
        sb.append(")");
        sb.append(" AS ");
    }

}
