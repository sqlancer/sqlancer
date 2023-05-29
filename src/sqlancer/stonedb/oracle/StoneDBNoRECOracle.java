package sqlancer.stonedb.oracle;

import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;

public class StoneDBNoRECOracle extends NoRECBase<StoneDBGlobalState> implements TestOracle<StoneDBGlobalState> {
    public StoneDBNoRECOracle(StoneDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {

    }
}
