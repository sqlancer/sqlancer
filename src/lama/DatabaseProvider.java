package lama;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.Main.StateToReproduce;

public interface DatabaseProvider {

	void generateAndTestDatabase(final String databaseName, Connection con, StateLogger logger, StateToReproduce state, QueryManager manager, MainOptions options) throws SQLException;

	Connection createDatabase(String databaseName) throws SQLException;

	String getLogFileSubdirectoryName();
}
