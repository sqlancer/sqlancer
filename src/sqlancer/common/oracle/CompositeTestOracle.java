package sqlancer.common.oracle;

import java.util.List;

import sqlancer.GlobalState;

public class CompositeTestOracle implements TestOracle {

    private final TestOracle[] oracles;
    private final GlobalState<?, ?> globalState;
    private int i;

    public CompositeTestOracle(List<TestOracle> oracles, GlobalState<?, ?> globalState) {
        this.globalState = globalState;
        this.oracles = oracles.toArray(new TestOracle[oracles.size()]);
    }

    @Override
    public void check() throws Exception {
        try {
            oracles[i].check();
            boolean lastOracleIndex = i == oracles.length - 1;
            if (!lastOracleIndex) {
                globalState.getManager().incrementSelectQueryCount();
            }
        } finally {
            i = (i + 1) % oracles.length;
        }
    }
}
