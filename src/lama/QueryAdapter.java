package lama;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryAdapter extends Query {
	
	private final String query;
	private final List<String> expectedErrors;

	public QueryAdapter(String query) {
		this(query, new ArrayList<>());
	}
	
	public QueryAdapter(String query, List<String> expectedErrors) {
		this.query = query;
		this.expectedErrors = expectedErrors;
		
	}

	@Override
	public String getQueryString() {
		return query;
	}

	@Override
	public void execute(Connection con) throws SQLException {
		try (Statement s = con.createStatement()) {
			s.execute(query);
		} catch (Exception e) {
			boolean isExcluded = false;
			for (String expectedError : expectedErrors) {
				if (e.getMessage().contains(expectedError)) {
					isExcluded = true;
					break;
				}
			}
			if (!isExcluded) {
				throw e;
			}
		}
	}

	@Override
	public boolean couldAffectSchema() {
		return false;
	}
	
}
