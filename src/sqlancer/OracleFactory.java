package sqlancer;

import java.sql.SQLException;

import sqlancer.common.oracle.TestOracle;

public interface OracleFactory<G extends GlobalState<?, ?>> {

    TestOracle create(G globalState) throws SQLException;

}
