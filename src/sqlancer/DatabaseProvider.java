package sqlancer;

import java.sql.Connection;

import sqlancer.common.log.LoggableFactory;

public interface DatabaseProvider<G extends GlobalState<O, ?>, O extends DBMSSpecificOptions<?>> {

    /**
     * Gets the the {@link GlobalState} class.
     *
     * @return the class extending {@link GlobalState}
     */
    Class<G> getGlobalStateClass();

    /**
     * Gets the JCommander option class.
     *
     * @return the class representing the DBMS-specific options.
     */
    Class<O> getOptionClass();

    /**
     * Generates a single database and executes a test oracle a given number of times.
     *
     * @param globalState
     *            the state created and is valid for this method call.
     *
     */
    void generateAndTestDatabase(G globalState) throws Exception;

    Connection createDatabase(G globalState) throws Exception;

    /**
     * The DBMS name is used to name the log directory and command to test the respective DBMS.
     *
     * @return the DBMS' name
     */
    String getDBMSName();

    LoggableFactory getLoggableFactory();

    StateToReproduce getStateToReproduce(String databaseName);

}
