package sqlancer.cnosdb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.cnosdb.oracle.CnosDBNoRECOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPAggregateOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPHavingOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPWhereOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;

public enum CnosDBOracleFactory implements OracleFactory<CnosDBGlobalState> {
    NOREC {
        @Override
        public TestOracle<CnosDBGlobalState> create(CnosDBGlobalState globalState) {
            return new CnosDBNoRECOracle(globalState);
        }
    },
    HAVING {
        @Override
        public TestOracle<CnosDBGlobalState> create(CnosDBGlobalState globalState) {
            return new CnosDBTLPHavingOracle(globalState);
        }

    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<CnosDBGlobalState> create(CnosDBGlobalState globalState) {
            List<TestOracle<CnosDBGlobalState>> oracles = new ArrayList<>();
            oracles.add(new CnosDBTLPWhereOracle(globalState));
            oracles.add(new CnosDBTLPHavingOracle(globalState));
            oracles.add(new CnosDBTLPAggregateOracle(globalState));
            return new CompositeTestOracle<>(oracles, globalState);
        }
    }

}
