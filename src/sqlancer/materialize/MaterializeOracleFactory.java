package sqlancer.materialize;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.materialize.gen.MaterializeCommon;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;
import sqlancer.materialize.oracle.MaterializePivotedQuerySynthesisOracle;
import sqlancer.materialize.oracle.tlp.MaterializeTLPAggregateOracle;
import sqlancer.materialize.oracle.tlp.MaterializeTLPHavingOracle;

public enum MaterializeOracleFactory implements OracleFactory<MaterializeGlobalState> {
    NOREC {
        @Override
        public TestOracle<MaterializeGlobalState> create(MaterializeGlobalState globalState) throws SQLException {
            MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(MaterializeCommon.getCommonExpressionErrors())
                    .with(MaterializeCommon.getCommonFetchErrors()).with("canceling statement due to statement timeout")
                    .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<MaterializeGlobalState> create(MaterializeGlobalState globalState) throws SQLException {
            return new MaterializePivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    WHERE {
        @Override
        public TestOracle<MaterializeGlobalState> create(MaterializeGlobalState globalState) throws SQLException {
            MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                    .with(MaterializeCommon.getCommonExpressionErrors()).with(MaterializeCommon.getCommonFetchErrors())
                    .build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    HAVING {
        @Override
        public TestOracle<MaterializeGlobalState> create(MaterializeGlobalState globalState) throws SQLException {
            return new MaterializeTLPHavingOracle(globalState);
        }

    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<MaterializeGlobalState> create(MaterializeGlobalState globalState) throws Exception {
            List<TestOracle<MaterializeGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(new MaterializeTLPAggregateOracle(globalState));
            return new CompositeTestOracle<MaterializeGlobalState>(oracles, globalState);
        }
    };

}
