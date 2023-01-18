package sqlancer.cnosdb;

import sqlancer.ExecutionTimer;
import sqlancer.GlobalState;
import sqlancer.cnosdb.client.CnosDBConnection;
import sqlancer.common.query.Query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CnosDBGlobalState extends GlobalState<CnosDBOptions, CnosDBSchema, CnosDBConnection> {

    public static final char IMMUTABLE = 'i';
    public static final char STABLE = 's';
    public static final char VOLATILE = 'v';

    private final Map<String, Character> functionsAndTypes = new HashMap<>();
    private List<Character> allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        if (success && getOptions().printSucceedingStatements()) {
            System.out.println(q.getQueryString());
        }
        if (logExecutionTime) {
            getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (q.couldAffectSchema()) {
            updateSchema();
        }
    }

    @Override
    public CnosDBSchema readSchema() throws Exception {
        return CnosDBSchema.fromConnection(getConnection());
    }

    public void addFunctionAndType(String functionName, Character functionType) {
        this.functionsAndTypes.put(functionName, functionType);
    }

    public Map<String, Character> getFunctionsAndTypes() {
        return this.functionsAndTypes;
    }

    public List<Character> getAllowedFunctionTypes() {
        return this.allowedFunctionTypes;
    }

}
