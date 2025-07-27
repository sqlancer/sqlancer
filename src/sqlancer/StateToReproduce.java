package sqlancer;

import java.io.Closeable;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sqlancer.common.query.Query;

public class StateToReproduce {

    public enum ErrorType {
        EXCEPTION, NOREC, TLP_WHERE
    }

    private List<Query<?>> statements = new ArrayList<>();

    private final String databaseName;

    private final DatabaseProvider<?, ?, ?> databaseProvider;

    private ErrorType errorType;

    private Map<String, Set<String>> expectedErrorsMap = new HashMap<>();

    private Map<String, String> reproducerData;

    private String exception;

    public String databaseVersion;

    protected long seedValue;

    public OracleRunReproductionState localState;

    private static class StateToReproduceSerializor {
        List<String> statements;
        String databaseName;
        String databaseProvider;
        ErrorType errorType;
        Map<String, Set<String>> expectedErrorsMap;
        Map<String, String> reproducerData;
        String exception;
    }

    public StateToReproduce(String databaseName, DatabaseProvider<?, ?, ?> databaseProvider) {
        this.databaseName = databaseName;
        this.databaseProvider = databaseProvider;
    }

    public void serialize(String fileName) {
        StateToReproduceSerializor serializor = new StateToReproduceSerializor();
        serializor.statements = this.statements.stream().map(Query::getLogString).collect(Collectors.toList());
        serializor.databaseName = this.databaseName;
        if (this.databaseProvider != null) {
            serializor.databaseProvider = this.databaseProvider.getClass().getName();
        }
        serializor.errorType = this.errorType;
        serializor.expectedErrorsMap = this.expectedErrorsMap;
        serializor.reproducerData = this.reproducerData;
        serializor.exception = this.exception;

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(gson.toJson(serializor));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static StateToReproduce deserialize(String fileName) {
        Gson gson = new Gson();
        StateToReproduceSerializor serializor = new StateToReproduceSerializor();
        try (FileReader reader = new FileReader(fileName)) {
            serializor = gson.fromJson(reader, StateToReproduceSerializor.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DatabaseProvider<?, ?, ?> provider = null;
        List<DatabaseProvider<?, ?, ?>> providers = Main.getDBMSProviders();
        for (DatabaseProvider<?, ?, ?> p : providers) {
            if (p.getClass().getName().equals(serializor.databaseProvider)) {
                provider = p;
                break;
            }
        }

        StateToReproduce state = new StateToReproduce(serializor.databaseName, provider);
        List<Query<?>> queries = new ArrayList<>();
        if (serializor.statements != null) {
            for (String s : serializor.statements) {
                queries.add(provider.getLoggableFactory().getQueryForStateToReproduce(s));
            }
        }
        state.setStatements(queries);
        state.setErrorType(serializor.errorType);
        state.setExpectedErrorsMap(serializor.expectedErrorsMap);
        state.setReproducerData(serializor.reproducerData);
        state.setException(serializor.exception);

        return state;
    }

    public void addExpectedError(String query, String errorMessage) {
        if (query != null && errorMessage != null) {
            expectedErrorsMap.computeIfAbsent(query, k -> new HashSet<>()).add(errorMessage);
        }
    }

    public String getException() {
        return exception;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public Map<String, String> getReproducerData() {
        return reproducerData;
    }

    public void setStatements(List<Query<?>> statements) {
        this.statements = statements;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public void setErrorType(ErrorType errorType) {
        this.errorType = errorType;
    }

    public void setExpectedErrorsMap(Map<String, Set<String>> expectedErrorsMap) {
        this.expectedErrorsMap = expectedErrorsMap;
    }

    public void setReproducerData(Map<String, String> reproducerData) {
        this.reproducerData = reproducerData;
    }

    /**
     * Logs the statement string without executing the corresponding statement.
     *
     * @param queryString
     *            the query string to be logged
     */
    public void logStatement(String queryString) {
        if (queryString == null) {
            throw new IllegalArgumentException();
        }
        logStatement(databaseProvider.getLoggableFactory().getQueryForStateToReproduce(queryString));
    }

    /**
     * Logs the statement without executing it.
     *
     * @param query
     *            the query to be logged
     */
    public void logStatement(Query<?> query) {
        if (query == null) {
            throw new IllegalArgumentException();
        }
        statements.add(query);
    }

    public List<Query<?>> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public void commentStatements() {
        for (int i = 0; i < statements.size(); i++) {
            Query<?> statement = statements.get(i);
            Query<?> newQuery = databaseProvider.getLoggableFactory().commentOutQuery(statement);
            statements.set(i, newQuery);
        }
    }

    public long getSeedValue() {
        return seedValue;
    }

    /**
     * Returns a local state in which a test oracle can save useful information about a single run. If the local state
     * is closed without indicating access to it, the local statements will be added to the global state.
     *
     * @return the local state for logging
     */
    public OracleRunReproductionState getLocalState() {
        return localState;
    }

    /**
     * State information that is logged if the test oracle finds a bug or if an exception is thrown.
     */
    public class OracleRunReproductionState implements Closeable {

        private final List<Query<?>> statements = new ArrayList<>();

        private boolean success;

        public OracleRunReproductionState() {
            StateToReproduce.this.localState = this;
        }

        public void executedWithoutError() {
            this.success = true;
        }

        public void log(String s) {
            statements.add(databaseProvider.getLoggableFactory().getQueryForStateToReproduce(s));
        }

        @Override
        public void close() {
            if (!success) {
                StateToReproduce.this.statements.addAll(statements);
            }

        }

    }

    public OracleRunReproductionState createLocalState() {
        return new OracleRunReproductionState();
    }
}
