package sqlancer;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface DatabaseProvider<G extends GlobalState> {
	
	void generateAndTestDatabase(G globalState) throws SQLException;

	G generateGlobalState();

	Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException;

	String getLogFileSubdirectoryName();

	void printDatabaseSpecificState(FileWriter writer, StateToReproduce state);

	StateToReproduce getStateToReproduce(String databaseName);

	public static boolean isEqualDouble(String first, String second) {
		try {
			double val = Double.parseDouble(first);
			double secVal = Double.parseDouble(second);
			return equals(val, secVal);
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean equals(double a, double b) {
		if (a == b)
			return true;
		// If the difference is less than epsilon, treat as equal.
		return Math.abs(a - b) < 0.0001 * Math.max(Math.abs(a), Math.abs(b));
	}


	public static List<String> getResultSetFirstColumnAsString(String queryString, Set<String> errors, Connection con) throws SQLException {
		QueryAdapter q = new QueryAdapter(queryString, errors);
		List<String> resultSet = new ArrayList<>();
		try (ResultSet result = q.executeAndGet(con)) {
			if (result == null) {
				throw new IgnoreMeException();
			}
			while (result.next()) {
				resultSet.add(result.getString(1));
			}
		} catch (Exception e) {
			if (e instanceof IgnoreMeException) {
				throw e;
			}
			for (String error : errors) {
				if (e.getMessage().contains(error)) {
					throw new IgnoreMeException();
				}
			}
			throw new AssertionError(queryString, e);
		}
		return resultSet;
	}
	
}
