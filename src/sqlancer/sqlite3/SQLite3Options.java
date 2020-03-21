package sqlancer.sqlite3;

import java.sql.SQLException;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import sqlancer.MainOptions.DBMSConverter;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.queries.SQLite3Fuzzer;
import sqlancer.sqlite3.queries.SQLite3MetamorphicAggregateTester;
import sqlancer.sqlite3.queries.SQLite3MetamorphicQuerySynthesizer;
import sqlancer.sqlite3.queries.SQLite3MetamorphicWindowSynthesizer;
import sqlancer.sqlite3.queries.SQLite3PivotedQuerySynthesizer;
import sqlancer.sqlite3.queries.SQLite3TestGenerator;

public class SQLite3Options {

	@Parameter(names = { "--test-fts" }, description = "Test the FTS extensions", arity = 1)
	public boolean testFts = true;

	@Parameter(names = { "--test-rtree" }, description = "Test the R*Tree extensions", arity = 1)
	public boolean testRtree = true;

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public SQLite3Oracle oracle = SQLite3Oracle.METAMORPHIC;

	public static enum SQLite3Oracle {
		PQS() {
			@Override
			public SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3PivotedQuerySynthesizer(globalState);
			}
		},
		METAMORPHIC() {
			@Override
			public SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3MetamorphicQuerySynthesizer(globalState);
			}
		},
		FUZZER() {
			@Override
			public SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3Fuzzer(globalState);
			}
		},
		WINDOW() {
			@Override
			public SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3MetamorphicWindowSynthesizer(globalState);
			}
		},
		AGGREGATE() {

			@Override
			public SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3MetamorphicAggregateTester(globalState);
			}
			
		}
 		;

		public abstract SQLite3TestGenerator create(SQLite3GlobalState globalState) throws SQLException;

	}

	public class SQLite3OracleConverter implements IStringConverter<SQLite3Oracle> {
		@Override
		public SQLite3Oracle convert(String value) {
			return SQLite3Oracle.valueOf(value);
		}
	}

}
