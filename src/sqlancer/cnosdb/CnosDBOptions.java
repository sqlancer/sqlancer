package sqlancer.cnosdb;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.cnosdb.CnosDBOptions.CnosDBOracleFactory;
import sqlancer.cnosdb.oracle.CnosDBNoRECOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPAggregateOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPHavingOracle;
import sqlancer.cnosdb.oracle.tlp.CnosDBTLPWhereOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;

@Parameters(separators = "=", commandDescription = "CnosDB (default port: " + CnosDBOptions.DEFAULT_PORT
        + ", default host: " + CnosDBOptions.DEFAULT_HOST + ")")
public class CnosDBOptions implements DBMSSpecificOptions<CnosDBOracleFactory> {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 31001;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for CnosDB")
    public List<CnosDBOracleFactory> oracle = List.of(CnosDBOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the CnosDB", arity = 1)
    public String connectionURL = String.format("http://%s:%d", CnosDBOptions.DEFAULT_HOST, CnosDBOptions.DEFAULT_PORT);

    @Override
    public List<CnosDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public enum CnosDBOracleFactory implements OracleFactory<CnosDBGlobalState> {
        NOREC {
            @Override
            public TestOracle<CnosDBGlobalState> create(CnosDBGlobalState globalState) {
                return new CnosDBNoRECOracle(globalState);
            }
        },

        // not support
        // PQS {
        // @Override
        // public TestOracle create(CnosDBGlobalState globalState) {
        // return new CnosDBPivotedQuerySynthesisOracle(globalState);
        // }
        //
        // @Override
        // public boolean requiresAllTablesToContainRows() {
        // return true;
        // }
        // },
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

}
