package sqlancer.duckdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.test.DuckDBNoRECOracle;
import sqlancer.duckdb.test.DuckDBQueryPartitioningAggregateTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningDistinctTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningHavingTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningWhereTester;

@Parameters
public class DuckDBOptions {
	
	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public List<DuckDBOracle> oracle = Arrays.asList(DuckDBOracle.QUERY_PARTITIONING);

	public static enum DuckDBOracle {
		NOREC {

			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBNoRECOracle(globalState);
			}
			
		},
		HAVING {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningHavingTester(globalState);
			}
		},
		WHERE {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningWhereTester(globalState);
			}
		},
		AGGREGATE {

			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningAggregateTester(globalState);
			}
			
		},
		DISTINCT {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningDistinctTester(globalState);
			}
		},	
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				List<TestOracle> oracles = new ArrayList<>();
				oracles.add(new DuckDBQueryPartitioningWhereTester(globalState));
				oracles.add(new DuckDBQueryPartitioningHavingTester(globalState));
				oracles.add(new DuckDBQueryPartitioningAggregateTester(globalState));
				return new CompositeTestOracle(oracles);
			}
		};

		public abstract TestOracle create(DuckDBGlobalState globalState) throws SQLException;

	}


}
