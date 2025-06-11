package sqlancer.oxla.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;

public abstract class OxlaQueryGenerator {
    /**
     * Generates a new query.
     *
     * @param globalState Current database state.
     * @param depth Starting depth of an expression generator (will nest expressions as long as depth < max depth).
     * @return Query corresponding to a given Generator.
     */
    public abstract SQLQueryAdapter getQuery(OxlaGlobalState globalState, int depth);

    /**
     * Notifies the Oracle if after this query SQLancer's database state is left in an inconsistent state, ex.
     * tables / roles / schemas were created or removed.
     */
    public boolean modifiesDatabaseState() {
        return false;
    }
}
