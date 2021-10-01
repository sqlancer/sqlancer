package sqlancer;

import sqlancer.common.log.LoggableFactory;

public interface DatabaseProvider<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

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
     * @throws Exception
     *             if creating the database fails.
     *
     */
    void generateAndTestDatabase(G globalState) throws Exception;

    /**
     * Reduce a state which triggers an error in a test oracle.
     *
     * @param e
     *          the exception carrying on the error.
     * @param stateToReduce
     *          the state to be reduced.
     * @param newGlobalState
     *          a copy state where to apply reduction.
     *          At the end of the reduction it contains a possibly reduced list of statements.
     * @throws Exception
     *          if creating the reduced database fails.
     */
    void reduceDatabase(FoundBugException e, G stateToReduce, G newGlobalState) throws Exception;

    C createDatabase(G globalState) throws Exception;

    /**
     * The DBMS name is used to name the log directory and command to test the respective DBMS.
     *
     * @return the DBMS' name
     */
    String getDBMSName();

    LoggableFactory getLoggableFactory();

    StateToReproduce getStateToReproduce(String databaseName);

}
