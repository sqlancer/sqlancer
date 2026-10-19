package sqlancer.sqlite3.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

public class TestSQLite3Schema {

    @Test
    public void splitColumnDefinitionsIgnoresNestedCommasAndQuotes() {
        String sql = "create table t (a text, b generated always as (coalesce(a, ',') || \")\") stored, "
                + "c int collate nocase, unique(a, c))";
        assertEquals(List.of("a text", "b generated always as (coalesce(a, ',') || \")\") stored",
                "c int collate nocase", "unique(a, c)"), SQLite3Schema.splitColumnDefinitions(sql));
    }
}
