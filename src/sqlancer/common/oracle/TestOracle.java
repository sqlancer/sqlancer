package sqlancer.common.oracle;

import sqlancer.GlobalState;

public interface TestOracle<G extends GlobalState<?, ?, ?>> {

    void check() throws Exception;

}
