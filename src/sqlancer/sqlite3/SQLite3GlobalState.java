package sqlancer.sqlite3;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3GlobalState extends SQLGlobalState<SQLite3Options, SQLite3Schema> {

    @Override
    protected SQLite3Schema readSchema() throws SQLException {
        return SQLite3Schema.fromConnection(this);
    }

}
