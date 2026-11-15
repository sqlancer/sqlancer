package sqlancer.common.oracle;

import java.util.List;

import sqlancer.GlobalState;
import sqlancer.Reproducer;

public class CompositeTestOracle<G extends GlobalState<?, ?, ?>> implements TestOracle<G> {

    private final List<TestOracle<G>> oracles;
    private final G globalState;
    private int i;
    // the oracle that ran last, whether or not it failed
    private int iLast;

    public CompositeTestOracle(List<TestOracle<G>> oracles, G globalState) {
        this.globalState = globalState;
        this.oracles = oracles;
    }

    @Override
    public void check() throws Exception {
        try {
            iLast = i;
            oracles.get(i).check();
            boolean lastOracleIndex = i == oracles.size() - 1;
            if (!lastOracleIndex) {
                globalState.getManager().incrementSelectQueryCount();
            }
        } finally {
            i = (i + 1) % oracles.size();
        }
    }

    @Override
    public String getLastQueryString() {
        return oracles.get(iLast).getLastQueryString();
    }

    // the failing oracle's reproducer, so that --use-reducer works with several oracles
    @Override
    public Reproducer<G> getLastReproducer() {
        return oracles.get(iLast).getLastReproducer();
    }
}
