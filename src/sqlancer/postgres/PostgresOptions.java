package sqlancer.postgres;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.postgres.test.PostgresNoRECOracle;
import sqlancer.postgres.test.PostgresPivotedQuerySynthesisGenerator;
import sqlancer.postgres.test.PostgresQueryPartitioningAggregateTester;
import sqlancer.postgres.test.PostgresQueryPartitioningHavingTester;
import sqlancer.postgres.test.PostgresQueryPartitioningWhereTester;

public class PostgresOptions {

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public List<PostgresOracle> oracle = Arrays.asList(PostgresOracle.QUERY_PARTITIONING);

	public static enum PostgresOracle {
		NOREC {
			@Override
			public TestOracle create(PostgresGlobalState globalState) throws SQLException {
				return new PostgresNoRECOracle(globalState);
			}
		},
		PQS {
			@Override
			public TestOracle create(PostgresGlobalState globalState) throws SQLException {
				return new PostgresPivotedQuerySynthesisGenerator(globalState);
			}
		},
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(PostgresGlobalState globalState) throws SQLException {
				List<TestOracle> oracles = new ArrayList<>();
				oracles.add(new PostgresQueryPartitioningWhereTester(globalState));
				oracles.add(new PostgresQueryPartitioningHavingTester(globalState));
				oracles.add(new PostgresQueryPartitioningAggregateTester(globalState));
				return new CompositeTestOracle(oracles);
			}
		};

		public abstract TestOracle create(PostgresGlobalState globalState) throws SQLException;

	}

}