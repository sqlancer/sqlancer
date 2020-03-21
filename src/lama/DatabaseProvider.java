package lama;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.QueryManager;
import lama.Main.StateLogger;

public interface DatabaseProvider {

	void generateAndTestDatabase(final String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException;

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

	
}
