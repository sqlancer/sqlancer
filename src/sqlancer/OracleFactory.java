package sqlancer;

import java.sql.SQLException;

import sqlancer.common.oracle.TestOracle;

public interface OracleFactory<G extends GlobalState<?, ?>> {

    TestOracle create(G globalState) throws SQLException;

    /**
     * Indicates whether the test oracle requires that all tables (including views) contain at least one row.
     *
     * @return whether the test oracle requires at least one row per table
     */
    default boolean requiresAllTablesToContainRows() {
        return false;
    }

}
