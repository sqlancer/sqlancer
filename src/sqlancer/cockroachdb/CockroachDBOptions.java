package sqlancer.cockroachdb;

import java.sql.SQLException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPAggregateOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPDistinctOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPExtendedWhereOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPGroupByOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPHavingOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPJoinOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPOracle;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "Test CockroachDB")
public class CockroachDBOptions {

    @Parameter(names = "--oracle")
    public CockroachDBOracle oracle = CockroachDBOracle.NOREC;

    public enum CockroachDBOracle {
        NOREC {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBNoRECOracle(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPAggregateOracle(globalState);
            }

        },
        GROUP_BY {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPGroupByOracle(globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPWhereOracle(globalState);
            }
        },
        DISTINCT {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPDistinctOracle(globalState);
            }
        },
        EXTENDED_WHERE {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPExtendedWhereOracle(globalState);
            }
        },
        JOIN {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPJoinOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
                return new CockroachDBTLPOracle(globalState);
            }
        };

        public abstract TestOracle create(CockroachDBGlobalState globalState) throws SQLException;

    }

    @Parameter(names = {
            "--test-hash-indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Test TEMPORARY tables")
    public boolean testTempTables = true;

    @Parameter(names = {
            "--increased-vectorization" }, description = "Generate VECTORIZE=on with a higher probability (which found a number of bugs in the past)")
    public boolean makeVectorizationMoreLikely = true;

}
