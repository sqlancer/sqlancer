package sqlancer;

import sqlancer.common.query.Query;

public interface AbstractAction<G> {

    Query<?> getQuery(G globalState) throws Exception;

    /**
     * Specifies whether it makes sense to request a {@link Query}, when the previous call to {@link #getQuery(Object)}
     * returned a query that failed executing.
     *
     * @return whether retrying getting queries makes sense, if the first query failed executing.
     */
    default boolean canBeRetried() {
        return true;
    }

}
