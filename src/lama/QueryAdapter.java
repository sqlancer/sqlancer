package lama;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryAdapter extends Query {
	
	private final String query;
	private final List<String> expectedErrors;
	private final boolean couldAffectSchema;
	
	public QueryAdapter(String query) {
		this(query, new ArrayList<>());
	}
	
	public QueryAdapter(String query, boolean couldAffectSchema) {
		this(query, new ArrayList<>(), couldAffectSchema);
	}
	
	public QueryAdapter(String query, List<String> expectedErrors) {
		this.query = query;
		this.expectedErrors = expectedErrors;
		this.couldAffectSchema = false;
	}
	
	public QueryAdapter(String query, List<String> expectedErrors, boolean couldAffectSchema) {
		this.query = query;
		this.expectedErrors = expectedErrors;
		this.couldAffectSchema = couldAffectSchema;
	}

	@Override
	public String getQueryString() {
		return query;
	}

	@Override
	public boolean execute(Connection con) throws SQLException {
		try (Statement s = con.createStatement()) {
			s.execute(query);
			return true;
		} catch (Exception e) {
			if (e.getMessage().contains("generated column loop")) {
				throw new IgnoreMeException();
			}
			checkException(e);
			return false;
		}
	}

	protected void checkException(Exception e) throws AssertionError {
		boolean isExcluded = false;
		for (String expectedError : expectedErrors) {
			if (e.getMessage().contains(expectedError)) {
				isExcluded = true;
				break;
			}
		}
		if (!isExcluded) {
			throw new AssertionError(query, e);
		}
	}
	
	@Override
	public ResultSet executeAndGet(Connection con) throws SQLException {
		Statement s = con.createStatement();
		try {
			return s.executeQuery(query);
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
		return null;
	}

	@Override
	public boolean couldAffectSchema() {
		return couldAffectSchema;
	}

	@Override
	public List<String> getExpectedErrors() {
		return expectedErrors;
	}
	
}
