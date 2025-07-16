package reducer;

import java.io.*;
import java.util.*;
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

public class SimpleReducer {
    private int partitionNum = 2;
    private boolean observedChange = false;
    private ReducerContext context;
    private DatabaseProvider<?, ?, ?> provider;

    public SimpleReducer(ReducerContext context) throws Exception {
        this.context = context;
        this.provider = createProvider(context.getProviderClassName());
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(1);
        }

        try {
            SimpleReducer reducer = new SimpleReducer();
            reducer.loadContext(args[0]);
            List<String> result = reducer.reduce();
            reducer.saveResults(result, args[1]);
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
        List<String> originalStatements = context.getSqlStatements();
        if (originalStatements == null || originalStatements.size() <= 1) {
            return originalStatements;
        }

        printInitialInfo(originalStatements);

        List<String> reducedStatements = new ArrayList<>(originalStatements);
        partitionNum = 2;
        int passCount = 1;

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

            start += subLength;
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
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean testExceptionStillExists(List<String> statements) {
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

    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean testOracleStillExists(List<String> statements) {
        try {
            DatabaseProvider<G, O, C> typedProvider = (DatabaseProvider<G, O, C>) provider;
            G globalState = createGlobalState(typedProvider);

            try (C connection = typedProvider.createDatabase(globalState)) {
                globalState.setConnection(connection);

                for (String sql : statements) {
                    try {
                        Query<C> query = (Query<C>) typedProvider.getLoggableFactory().getQueryForStateToReproduce(sql);
                        query.execute(globalState);
                    } catch (Throwable ignored) {
                        // ignore
                    }
                }

                return checkOracleStillTriggers(globalState, typedProvider);
            }
        } catch (Throwable e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean checkOracleStillTriggers(G globalState, DatabaseProvider<G, O, C> typedProvider) {
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

    private <G extends GlobalState<O, ?, SQLConnection>, O extends DBMSSpecificOptions<?>> boolean checkNoRECOracle(G globalState) {
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

            ExpectedErrors optimizedErrors = getExpectedErrors(optimizedQuery);
            ExpectedErrors unoptimizedErrors = getExpectedErrors(unoptimizedQuery);
            
            int optimizedResult = shouldUseAggregate ? extractCounts(optimizedQuery, optimizedErrors, globalState)
                    : countRows(optimizedQuery, optimizedErrors, globalState);
            int unoptimizedResult = extractCounts(unoptimizedQuery, unoptimizedErrors, globalState);

            return optimizedResult != -1 && unoptimizedResult != -1 && optimizedResult != unoptimizedResult;
        } catch (Throwable e) {
            return false;
        }
    }

    private <G extends SQLGlobalState<O, ?>, O extends DBMSSpecificOptions<?>> boolean checkTLPWhereOracle(G globalState) {
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

            ExpectedErrors originalErrors = getExpectedErrors(originalQuery);
            ExpectedErrors combinedErrors = getExpectedErrors(originalQuery, firstQuery, secondQuery, thirdQuery);

            List<String> combinedString = new ArrayList<>();
            List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, originalErrors, globalState);
            List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQuery, secondQuery, thirdQuery,
                    combinedString, !orderBy, globalState, combinedErrors);

            ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQuery, combinedString, globalState);
            return false;
        } catch (AssertionError e) {
            return true;
        } catch (Throwable e) {
            return false;
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

    private ExpectedErrors getExpectedErrors(String... queries) {
        ExpectedErrors expectedErrors = new ExpectedErrors();
        for (String query : queries) {
            Set<String> errorSet = context.getExpectedErrorsMap().get(query);
            if (errorSet != null) {
                expectedErrors.addAll(errorSet);
            }
        }
        return expectedErrors;
    }

    private <G extends GlobalState<?, ?, SQLConnection>> int countRows(String queryString, ExpectedErrors errors, G state) {
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
        if (currentError == null || originalError == null) return false;
        return currentError.contains(originalError) || originalError.contains(currentError);
    }

    private DatabaseProvider<?, ?, ?> createProvider(String providerClassName) throws Exception {
        return (DatabaseProvider<?, ?, ?>) Class.forName(providerClassName).getDeclaredConstructor().newInstance();
    }

    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> G createGlobalState(DatabaseProvider<G, O, C> typedProvider) throws Exception {
        G globalState = typedProvider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        MainOptions mainOptions = new MainOptions();
        globalState.setMainOptions(mainOptions);
        globalState.setStateLogger(new Main.StateLogger("temp", typedProvider, mainOptions));

        O dbmsSpecificOptions = typedProvider.getOptionClass().getDeclaredConstructor().newInstance();
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
        } catch (IOException e) {
            System.err.println("Failed to save results: " + e.getMessage());
        }
    }

    public SimpleReducer() {
    }
}