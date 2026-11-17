package sqlancer.risingwave;

import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.risingwave.gen.RisingWaveQueryGenerator;
import sqlancer.risingwave.oracle.tlp.RisingWaveTLPAggregateOracle;
import sqlancer.risingwave.oracle.tlp.RisingWaveTLPHavingOracle;

public enum RisingWaveOracleFactory implements OracleFactory<RisingWaveGlobalState> {
    WHERE {
        @Override
        public TestOracle<RisingWaveGlobalState> create(RisingWaveGlobalState state) {
            return new TLPWhereOracle<>(state, new RisingWaveQueryGenerator(state),
                    RisingWaveErrors.expressionErrors());
        }
    },
    HAVING {
        @Override
        public TestOracle<RisingWaveGlobalState> create(RisingWaveGlobalState state) {
            return new RisingWaveTLPHavingOracle(state);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<RisingWaveGlobalState> create(RisingWaveGlobalState state) {
            return new RisingWaveTLPAggregateOracle(state);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<RisingWaveGlobalState> create(RisingWaveGlobalState state) {
            return new CompositeTestOracle<>(
                    List.of(WHERE.create(state), HAVING.create(state), AGGREGATE.create(state)), state);
        }
    };

    @Override
    public abstract TestOracle<RisingWaveGlobalState> create(RisingWaveGlobalState state);
}
