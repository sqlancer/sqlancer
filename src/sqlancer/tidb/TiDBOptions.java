package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBOptions.TiDBOracleFactory;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.oracle.TiDBCERTOracle;
import sqlancer.tidb.oracle.TiDBTLPHavingOracle;
import sqlancer.tidb.oracle.TiDBTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions<TiDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = { "--max-num-tables" }, description = "The maximum number of tables/views that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--enable-non-prepared-plan-cache")
    public boolean nonPreparePlanCache;

    public enum TiDBOracleFactory implements OracleFactory<TiDBGlobalState> {
        HAVING {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPWhereOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                List<TestOracle<TiDBGlobalState>> oracles = new ArrayList<>();
                oracles.add(new TiDBTLPWhereOracle(globalState));
                oracles.add(new TiDBTLPHavingOracle(globalState));
                return new CompositeTestOracle<TiDBGlobalState>(oracles, globalState);
            }
        },
        CERT {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBCERTOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };

    }

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
