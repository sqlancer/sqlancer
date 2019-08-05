package lama.sqlite3.gen;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3DropTableGenerator {

	public static Query dropTable(SQLite3Schema newSchema) {
		if (newSchema.getDatabaseTablesWithoutViews().size() > 1) {
			Table tableToDrop = newSchema.getRandomTableNoView();
			String query = "DROP TABLE " + tableToDrop.getName();

			return new QueryAdapter(query, Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch", "Abort due to constraint violation (FOREIGN KEY constraint failed)", "SQL error or missing database")) {

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
