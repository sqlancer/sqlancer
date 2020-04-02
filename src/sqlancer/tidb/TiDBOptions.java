package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.test.TiDBQueryPartitioningHavingTester;
import sqlancer.tidb.test.TiDBQueryPartitioningWhereTester;

public class TiDBOptions {

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public List<TiDBOracle> oracle = Arrays.asList(TiDBOracle.QUERY_PARTITIONING);

	public static enum TiDBOracle {
		HAVING() {
			@Override
			public TestOracle create(TiDBGlobalState globalState) throws SQLException {
				return new TiDBQueryPartitioningHavingTester(globalState);
			}
		},
		WHERE() {
			@Override
			public TestOracle create(TiDBGlobalState globalState) throws SQLException {
				return new TiDBQueryPartitioningWhereTester(globalState);
			}
		},
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(TiDBGlobalState globalState) throws SQLException {
				List<TestOracle> oracles = new ArrayList<>();
				oracles.add(new TiDBQueryPartitioningWhereTester(globalState));
				oracles.add(new TiDBQueryPartitioningHavingTester(globalState));
				return new CompositeTestOracle(oracles);
			}
		};

		public abstract TestOracle create(TiDBGlobalState globalState) throws SQLException;

	}

}
