package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Query;
import lama.QueryAdapter;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3DropTableGenerator {

	public static Query dropTable(SQLite3Schema newSchema) {
		if (newSchema.getDatabaseTables().size() > 1) {
			Table tableToDrop = newSchema.getRandomTable();
			String query = "DROP TABLE " + tableToDrop.getName();

			return new QueryAdapter(query) {

				@Override
				public void execute(Connection con) throws SQLException {
					try {
						super.execute(con);
					} catch (SQLException e) {
						if (e.getMessage()
								.startsWith("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch")) {
							return;
						}
					}
				}

				@Override
				public boolean couldAffectSchema() {
					return true;
				}
			};
		} else {
			return new QueryAdapter("DROP TABLE IF EXISTS asdf");
		}
	}

}
