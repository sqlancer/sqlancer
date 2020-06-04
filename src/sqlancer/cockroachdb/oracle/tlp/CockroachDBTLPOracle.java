package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class CockroachDBTLPOracle implements TestOracle {

    private final TestOracle[] oracles;
    private int i;

    public CockroachDBTLPOracle(CockroachDBGlobalState state) {
        List<TestOracle> oracles = new ArrayList<>();
        oracles.add(new CockroachDBTLPAggregateOracle(state));
        oracles.add(new CockroachDBTLPHavingOracle(state));
        oracles.add(new CockroachDBTLPWhereOracle(state));
        oracles.add(new CockroachDBTLPGroupByOracle(state));
        oracles.add(new CockroachDBTLPExtendedWhereOracle(state));
        oracles.add(new CockroachDBTLPDistinctOracle(state));
        this.oracles = oracles.toArray(new TestOracle[4]);
    }

    @Override
    public void check() throws SQLException {
        oracles[i].check();
        i = (i + 1) % oracles.length;
    }

}
