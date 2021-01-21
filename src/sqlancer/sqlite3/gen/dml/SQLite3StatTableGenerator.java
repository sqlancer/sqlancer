package sqlancer.sqlite3.gen.dml;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table.TableKind;

public final class SQLite3StatTableGenerator {

    private final SQLite3GlobalState globalState;

    private SQLite3StatTableGenerator(SQLite3GlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(SQLite3GlobalState globalState) {
        return new SQLite3StatTableGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        List<SQLite3Column> columns = new ArrayList<>();
        SQLite3Table t = new SQLite3Table("sqlite_stat1", columns, TableKind.MAIN, false, false, false, false);
        if (Randomly.getBoolean()) {
            return SQLite3DeleteGenerator.deleteContent(globalState, t);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT OR IGNORE INTO sqlite_stat1");
            String indexName;
            try (Statement stat = globalState.getConnection().createStatement()) {
                try (ResultSet rs = stat
                        .executeQuery("SELECT name FROM sqlite_master WHERE type='index' ORDER BY RANDOM() LIMIT 1;")) {
                    if (rs.isClosed()) {
                        throw new IgnoreMeException();
                    }
                    indexName = rs.getString("name");
                }
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
            sb.append(" VALUES");
            sb.append("('");
            sb.append(globalState.getSchema().getRandomTable().getName());
            sb.append("', ");
            sb.append("'");
            if (Randomly.getBoolean()) {
                sb.append(indexName);
            } else {
                sb.append(globalState.getSchema().getRandomTable().getName());
            }
            sb.append("'");
            sb.append(", '");
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                if (i != 0) {
                    sb.append(" ");
                }
                if (Randomly.getBoolean()) {
                    sb.append(globalState.getRandomly().getInteger());
                } else {
                    sb.append(Randomly.smallNumber());
                }
            }
            if (Randomly.getBoolean()) {
                sb.append(" sz=");
                sb.append(globalState.getRandomly().getInteger());
            }
            if (Randomly.getBoolean()) {
                sb.append(" unordered");
            }
            if (Randomly.getBoolean()) {
                sb.append(" noskipscan");
            }
            sb.append("')");
            return new SQLQueryAdapter(sb.toString(),
                    ExpectedErrors.from("no such table", "The database file is locked"));
        }
    }

}
