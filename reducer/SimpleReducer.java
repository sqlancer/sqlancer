package reducer;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.sql.SQLException;

import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.query.Query;
import sqlancer.GlobalState;
import sqlancer.DatabaseProvider;
import sqlancer.ReducerContext;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.DBMSSpecificOptions;
import sqlancer.MainOptions;
import sqlancer.Main;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.IgnoreMeException;
import sqlancer.SQLGlobalState;
import sqlancer.SQLConnection;
import sqlancer.ComparatorHelper;

/**
 * SimpleReducer implements a partition-based SQL statement reduction algorithm similar to creduce. It systematically
 * removes unnecessary SQL statements while preserving the bug-triggering behavior for both exceptions and oracle-based
 * bugs.
 */
public class SimpleReducer {
    private int partitionNum = 2;
    private boolean observedChange = false;
    private ReducerContext context;
    private DatabaseProvider<?, ?, ?> provider;

    public SimpleReducer() {
    }

    public SimpleReducer(ReducerContext context) throws Exception {
        this.context = context;
        this.provider = createProvider(context.getProviderClassName());
    }

    public SimpleReducer(ReducerContext context, DatabaseProvider<?, ?, ?> provider) {
        this.context = context;
        this.provider = provider;
    }

    /**
     * Command-line entry point for SimpleReducer. Usage: java SimpleReducer <context_file> <output_file>
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(1);
        }

        String objectPath = args[0];
        String reducePath = args[1];

        try {
            SimpleReducer reducer = new SimpleReducer();
            reducer.loadContext(objectPath);
            List<String> result = reducer.reduce();
            reducer.saveResults(result, reducePath);

            System.out.println("Reduction completed successfully!");

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Performs the main reduction algorithm using partition-based delta debugging.
     *
     * @return List of reduced SQL statements that still trigger the bug
     */
    public List<String> reduce() throws Exception {
        validateState();

        List<String> originalStatements = context.getSqlStatements();
        if (originalStatements == null || originalStatements.size() <= 1) {
            System.out.println("Nothing to reduce (statements: "
                    + (originalStatements == null ? 0 : originalStatements.size()) + ")");
            return originalStatements;
        }

        printInitialInfo(originalStatements);

        List<String> reducedStatements = new ArrayList<>(originalStatements);
        partitionNum = 2;
        int passCount = 1;

        // Main reduction loop using partition-based algorithm
        while (reducedStatements.size() >= 2) {
            observedChange = false;
            System.out.print("Pass " + passCount + " (partition=" + partitionNum + "): ");

            List<String> result = tryReduction(reducedStatements);

            if (observedChange) {
                System.out.println(reducedStatements.size() + " -> " + result.size() + " statements");
                reducedStatements = result;
                passCount++;
            } else {
                System.out.println("no change");
                if (partitionNum == reducedStatements.size()) {
                    break;
                }
                partitionNum = Math.min(partitionNum * 2, reducedStatements.size());
            }
        }

        printFinalInfo(originalStatements, reducedStatements);
        return reducedStatements;
    }

    public void loadContext(String filePath) throws Exception {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            this.context = (ReducerContext) ois.readObject();
            this.context.setDatabaseName(this.context.getDatabaseName() + "-reduce");

            this.provider = (DatabaseProvider<?, ?, ?>) Class.forName(this.context.getProviderClassName())
                    .getDeclaredConstructor().newInstance();
        }
    }

    public void saveResults(List<String> reducedStatements, String logPath) {
        try {
            File logFile = new File(logPath);
            logFile.getParentFile().mkdirs();

            try (PrintWriter writer = new PrintWriter(new FileWriter(logFile))) {
                for (String statement : reducedStatements) {
                    writer.println(statement);
                }
            }

            System.out.println("Results saved to: " + logPath);
        } catch (IOException e) {
            System.err.println("Failed to save results: " + e.getMessage());
        }
    }

    public void setContext(ReducerContext context) {
        this.context = context;
    }

    public void setProvider(DatabaseProvider<?, ?, ?> provider) {
        this.provider = provider;
    }

    public ReducerContext getContext() {
        return context;
    }

    /**
     * Attempts reduction by partitioning statements and testing each partition removal.
     *
     * @return Reduced set of statements if successful, original set otherwise
     */
    private List<String> tryReduction(List<String> statements) throws Exception {
        List<String> currentStatements = statements;
        int start = 0;
        int subLength = statements.size() / partitionNum;

        while (start < statements.size()) {
            List<String> candidateStatements = new ArrayList<>(statements);
            int endPoint = Math.min(start + subLength, candidateStatements.size());
            candidateStatements.subList(start, endPoint).clear();

            if (testBugStillExists(candidateStatements)) {
                observedChange = true;
                currentStatements = candidateStatements;
                partitionNum = Math.max(partitionNum - 1, 2);
                break;
            }

            start = start + subLength;
        }

        return currentStatements;
    }

    private boolean testBugStillExists(List<String> statements) {
        try {
            switch (context.getErrorType()) {
            case EXCEPTION:
                return testExceptionStillExists(statements);
            case ORACLE:
                return testOracleStillExists(statements);
            default:
                return false;
            }
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Tests if the exception-based bug still occurs with reduced statements.
     *
     * @return true if the expected exception is thrown
     */
    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean testExceptionStillExists(
            List<String> statements) {
        try {
            DatabaseProvider<G, O, C> typedProvider = (DatabaseProvider<G, O, C>) provider;
            G globalState = createGlobalState(typedProvider);

            try (C connection = typedProvider.createDatabase(globalState)) {
                globalState.setConnection(connection);

                String originalErrorMessage = context.getErrorMessage();

                for (String sql : statements) {
                    try {
                        Query<C> query = (Query<C>) typedProvider.getLoggableFactory().getQueryForStateToReproduce(sql);
                        query.execute(globalState);
                    } catch (Throwable e) {
                        String currentErrorMessage = extractErrorMessage(e);
                        if (isErrorMatched(currentErrorMessage, originalErrorMessage)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Tests if the oracle-based bug still occurs with reduced statements.
     *
     * @return true if the oracle still detects inconsistency
     */
    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean testOracleStillExists(
            List<String> statements) {
        try {
            DatabaseProvider<G, O, C> typedProvider = (DatabaseProvider<G, O, C>) provider;
            G globalState = createGlobalState(typedProvider);

            try (C connection = typedProvider.createDatabase(globalState)) {
                globalState.setConnection(connection);

                // Execute all statements first
                for (String sql : statements) {
                    try {
                        Query<C> query = (Query<C>) typedProvider.getLoggableFactory().getQueryForStateToReproduce(sql);
                        query.execute(globalState);
                    } catch (Throwable ignored) {
                        // Ignore execution errors during setup
                    }
                }

                return checkOracleStillTriggers(globalState, typedProvider);
            }
        } catch (Throwable e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean checkOracleStillTriggers(
            G globalState, DatabaseProvider<G, O, C> typedProvider) {
        try {
            switch (context.getOracleType()) {
            case NOREC:
                return globalState.getConnection() instanceof SQLConnection
                        ? checkNoRECOracle((GlobalState<O, ?, SQLConnection>) globalState) : false;
            case TLP_WHERE:
                return globalState.getConnection() instanceof SQLConnection && globalState instanceof SQLGlobalState
                        ? checkTLPWhereOracle((SQLGlobalState<O, ?>) globalState) : false;
            default:
                return false;
            }
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Checks NoREC (Non-Equivalent Result Cardinality) oracle. Compares optimized vs unoptimized query results.
     */
    private <G extends GlobalState<O, ?, SQLConnection>, O extends DBMSSpecificOptions<?>> boolean checkNoRECOracle(
            G globalState) {
        try {
            Map<String, String> reproducerData = context.getReproducerData();
            if (reproducerData == null) {
                return false;
            }

            String optimizedQuery = reproducerData.get("optimizedQuery");
            String unoptimizedQuery = reproducerData.get("unoptimizedQuery");
            boolean shouldUseAggregate = Boolean
                    .parseBoolean(reproducerData.getOrDefault("shouldUseAggregate", "false"));

            if (optimizedQuery == null || unoptimizedQuery == null) {
                return false;
            }

            ExpectedErrors expectedErrors = createExpectedErrors();

            int optimizedResult = shouldUseAggregate ? extractCounts(optimizedQuery, expectedErrors, globalState)
                    : countRows(optimizedQuery, expectedErrors, globalState);

            int unoptimizedResult = extractCounts(unoptimizedQuery, expectedErrors, globalState);

            return optimizedResult != -1 && unoptimizedResult != -1 && optimizedResult != unoptimizedResult;
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Checks TLP WHERE (Three-Valued Logic Partitioning) oracle. Verifies that WHERE clause partitioning maintains
     * result set equivalence.
     */
    private <G extends SQLGlobalState<O, ?>, O extends DBMSSpecificOptions<?>> boolean checkTLPWhereOracle(
            G globalState) {
        try {
            Map<String, String> reproducerData = context.getReproducerData();
            if (reproducerData == null) {
                return false;
            }

            String firstQuery = reproducerData.get("firstQuery");
            String secondQuery = reproducerData.get("secondQuery");
            String thirdQuery = reproducerData.get("thirdQuery");
            String originalQuery = reproducerData.get("originalQuery");
            boolean orderBy = Boolean.parseBoolean(reproducerData.getOrDefault("orderBy", "false"));

            if (firstQuery == null || secondQuery == null || thirdQuery == null || originalQuery == null) {
                return false;
            }

            ExpectedErrors expectedErrors = createExpectedErrors();

            List<String> combinedString = new ArrayList<>();
            List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, expectedErrors,
                    globalState);
            List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQuery, secondQuery, thirdQuery,
                    combinedString, !orderBy, globalState, expectedErrors);

            ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQuery, combinedString,
                    globalState);
            return false;
        } catch (AssertionError e) {
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    private DatabaseProvider<?, ?, ?> createProvider(String providerClassName) throws Exception {
        return (DatabaseProvider<?, ?, ?>) Class.forName(providerClassName).getDeclaredConstructor().newInstance();
    }

    private void validateState() {
        if (context == null) {
            throw new IllegalStateException("Context not set. Use constructor with context or call setContext()");
        }
        if (provider == null) {
            throw new IllegalStateException("Provider not set. Use constructor with provider or call setProvider()");
        }
    }

    private void printInitialInfo(List<String> originalStatements) {
        System.out.println("Initial size: " + originalStatements.size() + " statements");
        String targetInfo = context.getErrorType() == ReducerContext.ErrorType.EXCEPTION
                ? "Target error: " + context.getErrorMessage() : "Target oracle: " + context.getOracleType();
        System.out.println(targetInfo);
        System.out.println();
    }

    private void printFinalInfo(List<String> originalStatements, List<String> reducedStatements) {
        System.out.println();
        System.out.println("Reduction completed:");
        System.out.println("Final size: " + reducedStatements.size() + " statements ("
                + String.format("%.1f", (1.0 - (double) reducedStatements.size() / originalStatements.size()) * 100)
                + "% reduction)");
    }

    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> G createGlobalState(
            DatabaseProvider<G, O, C> typedProvider) throws Exception {
        G globalState = typedProvider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        MainOptions mainOptions = new MainOptions();
        globalState.setMainOptions(mainOptions);
        globalState.setStateLogger(new Main.StateLogger("temp", typedProvider, mainOptions));

        O dbmsSpecificOptions = typedProvider.getOptionClass().getDeclaredConstructor().newInstance();

        // SQLite-specific configuration
        if ("sqlite3".equals(typedProvider.getDBMSName())) {
            try {
                dbmsSpecificOptions.getClass().getField("deleteIfExists").setBoolean(dbmsSpecificOptions, true);
                dbmsSpecificOptions.getClass().getField("generateDatabase").setBoolean(dbmsSpecificOptions, true);
            } catch (Exception ignored) {
            }
        }

        globalState.setDbmsSpecificOptions(dbmsSpecificOptions);
        StateToReproduce stateToRepro = typedProvider.getStateToReproduce(context.getDatabaseName());
        globalState.setState(stateToRepro);
        globalState.setDatabaseName(context.getDatabaseName());

        return globalState;
    }

    private ExpectedErrors createExpectedErrors() {
        ExpectedErrors expectedErrors = new ExpectedErrors();
        Set<String> errorSet = context.getExpectedErrors();
        if (errorSet != null) {
            for (String error : errorSet) {
                expectedErrors.add(error);
            }
        }
        return expectedErrors;
    }

    private <G extends GlobalState<?, ?, SQLConnection>> int countRows(String queryString, ExpectedErrors errors,
            G state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null)
                return -1;
            while (rs.next())
                count++;
        } catch (SQLException e) {
            return -1;
        } catch (IgnoreMeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

    private <G extends GlobalState<?, ?, SQLConnection>> int extractCounts(String queryString, ExpectedErrors errors,
            G state) {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors, false, false);
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null)
                return -1;
            while (rs.next())
                count += rs.getInt(1);
        } catch (SQLException e) {
            return -1;
        } catch (IgnoreMeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(q.getQueryString(), e);
        }
        return count;
    }

    private String extractErrorMessage(Throwable e) {
        Throwable rootCause = e;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }

        if (rootCause.getMessage() != null && !rootCause.getMessage().trim().isEmpty()) {
            return rootCause.getMessage();
        }

        if (e.getMessage() != null && !e.getMessage().trim().isEmpty()) {
            return e.getMessage();
        }

        return rootCause.getClass().getSimpleName();
    }

    private boolean isErrorMatched(String currentError, String originalError) {
        if (currentError == null || originalError == null)
            return false;
        return currentError.contains(originalError) || originalError.contains(currentError);
    }
}