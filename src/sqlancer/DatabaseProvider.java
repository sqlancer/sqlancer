package sqlancer;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseProvider<G extends GlobalState<O, ?>, O> {

    /**
     * Gets the the {@link GlobalState} class.
     */
    Class<G> getGlobalStateClass();

    /**
     * Gets the JCommander option class.
     */
    Class<O> getOptionClass();

    /**
     * Generates a single database and executes a test oracle a given number of times.
     *
     * @param globalState
     *            the state created and is valid for this method call.
     *
     */
    void generateAndTestDatabase(G globalState) throws SQLException;

    Connection createDatabase(G globalState) throws SQLException;

    /**
     * The DBMS name is used to name the log directory and command to test the respective DBMS.
     */
    String getDBMSName();

    // TODO: remove this
    /**
     * Deprecated method to print the database-specific state, previously used for PQS.
     *
     * @param writer
     * @param state
     */
    void printDatabaseSpecificState(FileWriter writer, StateToReproduce state);

    StateToReproduce getStateToReproduce(String databaseName);

}
