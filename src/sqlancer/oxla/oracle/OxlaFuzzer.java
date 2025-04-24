package sqlancer.oxla.oracle;

import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;

public class OxlaFuzzer implements TestOracle<OxlaGlobalState> {
    private final OxlaGlobalState globalState;

    public OxlaFuzzer(OxlaGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        try {
            globalState.executeStatement(new SQLQueryAdapter("SELECT 1"));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {
            throw new AssertionError(e);
        }

    }
}
