package sqlancer.sqlite3.queries;

import java.sql.SQLException;

public interface SQLite3TestGenerator {

	public void check() throws SQLException;

	public default boolean onlyWorksForNonEmptyTables() {
		return false;
	}
	
}
