package sqlancer.common.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
import sqlancer.QueryAdapter;

public abstract class AbstractGenerator {

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StringBuilder sb = new StringBuilder();
    protected boolean canAffectSchema;

    public Query getQuery() {
        buildStatement();
        return new QueryAdapter(sb.toString(), errors, canAffectSchema);
    }

    public abstract void buildStatement();

}
