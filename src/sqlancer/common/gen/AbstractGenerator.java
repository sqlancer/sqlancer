package sqlancer.common.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public abstract class AbstractGenerator {

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StringBuilder sb = new StringBuilder();
    protected boolean canAffectSchema;
    protected boolean canonicalizeString = true;

    public SQLQueryAdapter getStatement() {
        buildStatement();
        return new SQLQueryAdapter(sb.toString(), errors, canAffectSchema, canonicalizeString);
    }

    public abstract void buildStatement();

}
