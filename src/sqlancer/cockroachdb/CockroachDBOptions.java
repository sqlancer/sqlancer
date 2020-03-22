package sqlancer.cockroachdb;

import java.sql.SQLException;

import com.beust.jcommander.Parameter;

import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.test.CockroachDBQueryPartitioningAggregateTester;
import sqlancer.cockroachdb.test.CockroachDBNoRECTester;
import sqlancer.cockroachdb.test.CockroachDBNoTableTester;

public class CockroachDBOptions {
	
	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public CockroachDBOracle oracle = CockroachDBOracle.NOREC;

	public static enum CockroachDBOracle {
		NOREC() {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBNoRECTester(globalState);
			}
		},
		AGGREGATE() {

			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBQueryPartitioningAggregateTester(globalState);
			}
			
		},
		NOTABLE() {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBNoTableTester(globalState);
			}
		}
		;

		public abstract TestOracle create(CockroachDBGlobalState globalState) throws SQLException;

	}

}
