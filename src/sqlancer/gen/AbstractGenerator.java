package sqlancer.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;

public abstract class AbstractGenerator {

    protected final Set<String> errors = new HashSet<>();
    protected final StringBuilder sb = new StringBuilder();
    protected boolean canAffectSchema;

    public Query getQuery() {
        buildStatement();
        return new QueryAdapter(sb.toString(), errors, canAffectSchema);
    }

    public abstract void buildStatement();

}
