package sqlancer.oxla;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.oxla.oracle.OxlaFuzzer;

import java.util.ArrayList;
import java.util.List;

public enum OxlaOracleFactory implements OracleFactory<OxlaGlobalState> {
    NOREC {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return null;
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            List<TestOracle<OxlaGlobalState>> oracles = new ArrayList<>();
            // TODO
            return new CompositeTestOracle<>(oracles, globalState);
        }

    },
    FUZZER {
        @Override
        public TestOracle<OxlaGlobalState> create(OxlaGlobalState globalState) throws Exception {
            return new OxlaFuzzer(globalState);
        }
    }
}
