package sqlancer.common.oracle;

import java.sql.SQLException;

public interface TestOracle {

    void check() throws SQLException;

    default boolean onlyWorksForNonEmptyTables() {
        return false;
    }

}
