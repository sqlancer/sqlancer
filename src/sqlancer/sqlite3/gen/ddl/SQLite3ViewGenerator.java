package sqlancer.sqlite3.gen.ddl;

import java.sql.SQLException;

import sqlancer.ExpectedErrors;
import sqlancer.IgnoreMeException;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.oracle.SQLite3PivotedQuerySynthesisOracle;
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
        errors.add("is circularly defined");
        errors.add("unsupported frame specification");
        if (Randomly.getBoolean()) {
            SQLite3PivotedQuerySynthesisOracle queryGen = new SQLite3PivotedQuerySynthesisOracle(globalState);
            try {
                SQLite3Select q = queryGen.getQuery(globalState);
                // for (SQLite3Expression expr : q.getFetchColumns()) {
                // if (expr.getAffinity() != null || expr.getImplicitCollateSequence() != null ||
                // expr.getExplicitCollateSequence() != null) {
                // throw new IgnoreMeException();
                // }
                // }
                if (!globalState.getDmbsSpecificOptions().testDistinctInView) {
                    q.setSelectType(SelectType.ALL);
                }
                int size = q.getFetchColumns().size();
                columnNamesAs(sb, size);
                sb.append(SQLite3Visitor.asString(q));
                SQLite3PivotedQuerySynthesisOracle.addExpectedErrors(errors);
                return new QueryAdapter(sb.toString(), errors, true);
            } catch (AssertionError e) {
                throw new IgnoreMeException();
            }
        } else {
            int size = 1 + Randomly.smallNumber();
            columnNamesAs(sb, size);
            SQLite3Expression randomQuery = SQLite3RandomQuerySynthesizer.generate(globalState, size);
            sb.append(SQLite3Visitor.asString(randomQuery));
            SQLite3PivotedQuerySynthesisOracle.addExpectedErrors(errors);
            return new QueryAdapter(sb.toString(), errors, true);
        }

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
