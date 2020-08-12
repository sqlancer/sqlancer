package sqlancer;

import java.sql.SQLException;

public interface OracleFactory<G extends GlobalState<?, ?>> {

    TestOracle create(G globalState) throws SQLException;

}
