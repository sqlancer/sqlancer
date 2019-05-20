package lama;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class Query {

	public abstract String getQueryString();

	/**
	 * Whether the query could affect the schema (i.e., by add/deleting columns or tables).
	 * @return
	 */
	public abstract boolean couldAffectSchema();

	public abstract void execute(Connection con) throws SQLException;

	@Override
	public String toString() {
		return getQueryString();
	}

}
