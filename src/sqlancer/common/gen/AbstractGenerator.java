package sqlancer.common.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;

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
