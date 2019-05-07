package lama;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryAdapter extends Query {
	
	private final String query;

	public QueryAdapter(String query) {
		this.query = query;
		
	}

	@Override
	public String getQueryString() {
		return query;
	}

	@Override
	public void execute(Connection con) throws SQLException {
		try (Statement s = con.createStatement()) {
			s.execute(query);
		}
	}

}
