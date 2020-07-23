package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.CompositeTestOracle;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.oracle.TiDBTLPHavingOracle;
import sqlancer.tidb.oracle.TiDBTLPWhereOracle;

@Parameters
public class TiDBOptions {

    @Parameter(names = "--oracle")
    public List<TiDBOracle> oracle = Arrays.asList(TiDBOracle.QUERY_PARTITIONING);

    public enum TiDBOracle {
        HAVING {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPWhereOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new TiDBTLPWhereOracle(globalState));
                oracles.add(new TiDBTLPHavingOracle(globalState));
                return new CompositeTestOracle(oracles);
            }
        };

        public abstract TestOracle create(TiDBGlobalState globalState) throws SQLException;

    }

}
