package sqlancer;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface DatabaseProvider<G extends GlobalState<O>, O> {
	
	void generateAndTestDatabase(G globalState) throws SQLException;

	G generateGlobalState();

	// TODO: should be type G
	Connection createDatabase(GlobalState<?> globalState) throws SQLException;

	
	/**
	 * The DBMS name is used to name the log directory and command to test the respective DBMS.
	 * 
	 * @return
	 */
	String getDBMSName();

	
	// TODO: remove this
	default void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		
	}

	StateToReproduce getStateToReproduce(String databaseName);
	
	O getCommand();

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


	public static List<String> getResultSetFirstColumnAsString(String queryString, Set<String> errors, Connection con, GlobalState<?> state) throws SQLException {
		if (state.getOptions().logEachSelect()) {
			// TODO: refactor me
			state.getLogger().writeCurrent(queryString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		QueryAdapter q = new QueryAdapter(queryString, errors);
		List<String> resultSet = new ArrayList<>();
		ResultSet result = null;
		try {
			result = q.executeAndGet(con);
			if (result == null) {
				throw new IgnoreMeException();
			}
			while (result.next()) {
				resultSet.add(result.getString(1));
			}
			result.getStatement().close();
		} catch (Exception e) {
			if (e instanceof IgnoreMeException) {
				throw e;
			}
			if (e instanceof NumberFormatException) {
				// https://github.com/tidb-challenge-program/bug-hunting-issue/issues/57
				throw new IgnoreMeException();
			}
			if (e.getMessage() == null) {
				throw new AssertionError(queryString, e);
			}
			for (String error : errors) {
				if (e.getMessage().contains(error)) {
					throw new IgnoreMeException();
				}
			}
			throw new AssertionError(queryString, e);
		} finally {
			if (result != null && !result.isClosed()) {
				result.getStatement().close();
				result.close();
			}
		}
		return resultSet;
	}
	
}
