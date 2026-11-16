package sqlancer.sqlite3.gen.dml;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import sqlancer.IgnoreMeException;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table.TableKind;

class TestSQLite3DeleteGenerator {

    @Test
    void deleteIsSkippedWhenNoWritableTablesExist() {
        TestGlobalState state = new TestGlobalState();
        SQLite3Table view = new SQLite3Table("v", Collections.emptyList(), TableKind.MAIN, false, true, false, false);
        state.setSchemaForTesting(new SQLite3Schema(Collections.singletonList(view), Collections.emptyList()));

        assertThrows(IgnoreMeException.class, () -> SQLite3DeleteGenerator.deleteContent(state));
    }

    private static final class TestGlobalState extends SQLite3GlobalState {
        void setSchemaForTesting(SQLite3Schema schema) {
            setSchema(schema);
        }
    }
}
