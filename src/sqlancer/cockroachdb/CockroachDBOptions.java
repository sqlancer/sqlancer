package sqlancer.cockroachdb;

import java.sql.SQLException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.test.CockroachDBNoRECTester;
import sqlancer.cockroachdb.test.CockroachDBNoTableTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningAggregateTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningExtendedWhereTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningHavingTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningJoinTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningTester;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningWhereTester;

@Parameters(separators = "=", commandDescription = "Test CockroachDB")
public class CockroachDBOptions {

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public CockroachDBOracle oracle = CockroachDBOracle.NOREC;

	public static enum CockroachDBOracle {
		NOREC {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBNoRECTester(globalState);
			}
		},
		AGGREGATE {

			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningAggregateTester(globalState);
			}

		},
		NOTABLE {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBNoTableTester(globalState);
			}
		},
		HAVING {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningHavingTester(globalState);
			}
		},
		WHERE {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningWhereTester(globalState);
			}
		},
		EXTENDED_WHERE {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningExtendedWhereTester(globalState);
			}
		},
		JOIN {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningJoinTester(globalState);
			}
		},
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningTester(globalState);
			}
		};

		public abstract TestOracle create(CockroachDBGlobalState globalState) throws SQLException;

	}

	@Parameter(names = {
			"--test_hash_indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
	public boolean testHashIndexes = true;

	@Parameter(names = { "--test_temp_tables" }, description = "Test TEMPORARY tables")
	public boolean testTempTables = true;

	@Parameter(names = { "--increased_vectorization",
			"Generate VECTORIZE=on with a higher probability (which found a number of bugs in the past)" })
	public boolean makeVectorizationMoreLikely = true;

}
