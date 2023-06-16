package sqlancer.reducer;

import sqlancer.*;
import sqlancer.common.query.Query;
import sqlancer.reducer.VirtualDB.VirtualDBGlobalState;
import sqlancer.reducer.VirtualDB.VirtualDBProvider;
import sqlancer.reducer.VirtualDB.VirtualDBQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: Make Connection a generic type OR Fake a conn QUERY AND CONNECTION BOTH ARE FAKE. FAKE QUERY sub class
 */
public class TestEnvironment {
    private final String databaseName = "virtual_db";
    private final MainOptions options = new MainOptions();
    private VirtualDBProvider provider = null;
    private VirtualDBGlobalState state, newGlobalState;

    public TestEnvironment() throws Exception {
        setUpTestingEnvironment();
    }

    /**
     * @param queries:
     *            List of Query<?>
     *
     * @return String of queries that appended together with '\n' separated (no '\n' at the last line)
     */
    public static String getQueriesString(List<Query<?>> queries) {
        return queries.stream().map(Query::getQueryString).collect(Collectors.joining("\n"));
    }

    private VirtualDBGlobalState createGlobalState() {
        try {
            return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("rawtypes")
    private void initVirtualDBProvider() {
        try {
            ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
            for (DatabaseProvider<?, ?, ?> provider : loader) {
                if (provider.getDBMSName().equals(databaseName)) {
                    this.provider = (VirtualDBProvider) provider;
                    break;
                }
            }
            if (provider == null) {
                throw new AssertionError("testing provider not registered");
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    private void setUpTestingEnvironment() throws Exception {
        initVirtualDBProvider();
        state = createGlobalState();
        StateToReproduce stateToReproduce = provider.getStateToReproduce(databaseName);

        state.setState(stateToReproduce);
        state.setDatabaseName(databaseName);
        state.setMainOptions(options);

        // Main.StateLogger logger = new Main.StateLogger(databaseName, provider, options);
        // state.setStateLogger(logger);

        try (SQLConnection con = provider.createDatabase(state)) {
            state.setConnection(con);
            newGlobalState = createGlobalState();
            // Main.StateLogger newLogger = new Main.StateLogger(databaseName, provider, options);
            // newGlobalState.setStateLogger(newLogger);
            newGlobalState.setState(stateToReproduce);
            newGlobalState.setDatabaseName(databaseName);
            newGlobalState.setMainOptions(options);
        }
    }

    public void setInitialStatementsFromStrings(List<String> statements) {
        List<Query<?>> queries = new ArrayList<>();
        for (String s : statements) {
            queries.add(new VirtualDBQuery(s));
        }
        state.getState().setStatements(queries);
    }

    public void setBugInducingCondition(Function<List<Query<?>>, Boolean> bugInducingCondition) {
        state.setBugInducingCondition(bugInducingCondition);
        newGlobalState.setBugInducingCondition(bugInducingCondition);
    }

    public void runReduce() throws Exception {
        Reducer<VirtualDBGlobalState> reducer = new StatementReducer<>(provider);
        Reproducer<VirtualDBGlobalState> reproducer = provider.generateAndTestDatabase(newGlobalState);
        reducer.reduce(state, reproducer, newGlobalState);
    }

    public List<Query<?>> getReducedStatements() {
        return newGlobalState.getState().getStatements();
    }

    public List<Query<?>> getInitialStatements() {
        return state.getState().getStatements();
    }
}
