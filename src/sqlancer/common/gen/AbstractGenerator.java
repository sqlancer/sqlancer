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

    /**
     * Appends {@code  WHERE <condition>} (with a leading space). Subclasses are responsible for deciding whether to
     * include the WHERE clause, typically based on a randomized boolean. Used by DELETE, UPDATE, partial-INDEX, and
     * INSERT...ON CONFLICT generators.
     *
     * @param condition
     *            the rendered WHERE condition.
     */
    protected void appendWhereClause(String condition) {
        sb.append(" WHERE ");
        sb.append(condition);
    }

}
