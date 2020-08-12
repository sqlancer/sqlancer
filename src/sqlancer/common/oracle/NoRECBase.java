package sqlancer.common.oracle;

import java.sql.Connection;

import sqlancer.GlobalState;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.common.query.ExpectedErrors;

public abstract class NoRECBase<S extends GlobalState<?, ?>> implements TestOracle {

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final Connection con;
    protected String optimizedQueryString;
    protected String unoptimizedQueryString;

    public NoRECBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }

}
