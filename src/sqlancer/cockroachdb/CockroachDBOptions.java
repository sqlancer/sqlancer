package sqlancer.cockroachdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.cockroachdb.CockroachDBOptions.CockroachDBOracleFactory;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPAggregateOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPDistinctOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPExtendedWhereOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPGroupByOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPHavingOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPWhereOracle;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;

@Parameters(separators = "=", commandDescription = "CockroachDB (default port: " + CockroachDBOptions.DEFAULT_PORT
        + " default host: " + CockroachDBOptions.DEFAULT_HOST + ")")
public class CockroachDBOptions implements DBMSSpecificOptions<CockroachDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 26257;

    @Parameter(names = "--oracle")
    public CockroachDBOracleFactory oracle = CockroachDBOracleFactory.NOREC;

    public enum CockroachDBOracleFactory implements OracleFactory<CockroachDBGlobalState> {
        NOREC {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBNoRECOracle(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPAggregateOracle(globalState);
            }

        },
        GROUP_BY {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPGroupByOracle(globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPWhereOracle(globalState);
            }
        },
        DISTINCT {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPDistinctOracle(globalState);
            }
        },
        EXTENDED_WHERE {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPExtendedWhereOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<CockroachDBGlobalState> create(CockroachDBGlobalState globalState) throws SQLException {
                List<TestOracle<CockroachDBGlobalState>> oracles = new ArrayList<>();
                oracles.add(new CockroachDBTLPAggregateOracle(globalState));
                oracles.add(new CockroachDBTLPHavingOracle(globalState));
                oracles.add(new CockroachDBTLPWhereOracle(globalState));
                oracles.add(new CockroachDBTLPGroupByOracle(globalState));
                oracles.add(new CockroachDBTLPExtendedWhereOracle(globalState));
                oracles.add(new CockroachDBTLPDistinctOracle(globalState));
                return new CompositeTestOracle<CockroachDBGlobalState>(oracles, globalState);
            }
        };

    }

    @Parameter(names = {
            "--test-hash-indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Test TEMPORARY tables")
    public boolean testTempTables; // default: false https://github.com/cockroachdb/cockroach/issues/85388

    @Parameter(names = { "--max-num-tables" }, description = "The maximum number of tables that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Override
    public List<CockroachDBOracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracle);
    }

}
