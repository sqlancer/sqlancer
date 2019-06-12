package lama;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.StateLogger;

public interface DatabaseProvider {

	void generateAndTestDatabase(final String databaseName, Connection con, StateLogger logger) throws SQLException;

	Connection createDatabase(String databaseName) throws SQLException;
}
