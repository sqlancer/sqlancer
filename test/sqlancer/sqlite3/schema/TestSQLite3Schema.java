package sqlancer.sqlite3.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.SQLConnection;
import sqlancer.sqlite3.SQLite3GlobalState;

public class TestSQLite3Schema {

    @Test
    public void splitColumnDefinitionsIgnoresNestedCommasAndQuotes() {
        String sql = "create table t (a text, b generated always as (coalesce(a, ',') || \")\") stored, "
                + "c int collate nocase, unique(a, c))";
        assertEquals(List.of("a text", "b generated always as (coalesce(a, ',') || \")\") stored",
                "c int collate nocase", "unique(a, c)"), SQLite3Schema.splitColumnDefinitions(sql));
    }

    @Test
    public void schemaParsingHandlesViewsWithoutExplicitColumns() throws Exception {
        SQLite3GlobalState state = new SQLite3GlobalState();
        try (SQLConnection connection = new SQLConnection(DriverManager.getConnection("jdbc:sqlite::memory:"))) {
            state.setConnection(connection);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (a TEXT, b INTEGER)");
                statement.execute("CREATE VIEW v AS SELECT * FROM t");
            }

            SQLite3Schema schema = SQLite3Schema.fromConnection(state);
            assertEquals(2, schema.getDatabaseTable("v").getColumns().size());
        }
    }
}
