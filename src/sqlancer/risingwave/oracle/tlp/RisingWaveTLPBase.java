package sqlancer.risingwave.oracle.tlp;

import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.risingwave.RisingWaveErrors;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.gen.RisingWaveQueryGenerator;

public abstract class RisingWaveTLPBase
        extends TernaryLogicPartitioningOracleBase<RisingWaveExpression, RisingWaveGlobalState> {
    protected RisingWaveQueryGenerator gen;

    protected RisingWaveTLPBase(RisingWaveGlobalState state) {
        super(state);
        RisingWaveErrors.addExpressionErrors(errors);
    }

    @Override
    protected RisingWaveQueryGenerator getGen() {
        return gen;
    }
}
