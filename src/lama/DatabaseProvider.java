package lama;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.QueryManager;
import lama.Main.StateLogger;

public interface DatabaseProvider {
	

	void generateAndTestDatabase(final String databaseName, Connection con, StateLogger logger, StateToReproduce state, QueryManager manager, MainOptions options) throws SQLException;

	Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException;

	String getLogFileSubdirectoryName();

	void printDatabaseSpecificState(FileWriter writer, StateToReproduce state);

	StateToReproduce getStateToReproduce(String databaseName);

	Query checkIfRowIsStillContained(StateToReproduce state);
}
