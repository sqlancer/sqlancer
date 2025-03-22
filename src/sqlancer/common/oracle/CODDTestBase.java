package sqlancer.common.oracle;

import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;

public abstract class CODDTestBase<S extends SQLGlobalState<?, ?>> implements TestOracle<S> {
    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String auxiliaryQueryString;
    protected String foldedQueryString;
    protected String originalQueryString;

    public CODDTestBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }
}