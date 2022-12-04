package sqlancer.common.oracle;

import sqlancer.GlobalState;
import sqlancer.Reproducer;

public interface TestOracle<G extends GlobalState<?, ?, ?>> {

    void check() throws Exception;

    default Reproducer<G> getLastReproducer() {
        return null;
    }
}
