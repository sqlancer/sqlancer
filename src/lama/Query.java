package lama;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class Query {

	public abstract String getQueryString();
	public abstract void execute(Connection con) throws SQLException;
	
	@Override
	public String toString() {
		return getQueryString();
	}
	
}
