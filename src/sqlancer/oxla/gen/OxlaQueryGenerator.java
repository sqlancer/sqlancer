package sqlancer.oxla.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;

public abstract class OxlaQueryGenerator {
    protected OxlaExpressionGenerator generator;
    protected OxlaGlobalState globalState;

    public OxlaQueryGenerator(OxlaGlobalState globalState) {
        this.globalState = globalState;
        this.generator = new OxlaExpressionGenerator(globalState);
    }

    public abstract SQLQueryAdapter getQuery(int depth);
}
