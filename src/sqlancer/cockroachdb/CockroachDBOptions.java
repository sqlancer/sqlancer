package sqlancer.cockroachdb;

import java.sql.SQLException;

import com.beust.jcommander.Parameter;

import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.test.CockroachDBMetamorphicAggregateTester;
import sqlancer.cockroachdb.test.CockroachDBMetamorphicQuerySynthesizer;
import sqlancer.cockroachdb.test.CockroachDBNoTableTester;

public class CockroachDBOptions {
	
	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public CockroachDBOracle oracle = CockroachDBOracle.NOREC;

	public static enum CockroachDBOracle {
		NOREC() {
			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBMetamorphicQuerySynthesizer(globalState);
			}
		},
		AGGREGATE() {

			@Override
			public TestOracle create(CockroachDBGlobalState globalState) throws SQLException {
				return new CockroachDBMetamorphicAggregateTester(globalState);
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
