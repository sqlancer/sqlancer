package sqlancer.sqlite3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.queries.SQLite3Fuzzer;
import sqlancer.sqlite3.queries.SQLite3MetamorphicQuerySynthesizer;
import sqlancer.sqlite3.queries.SQLite3PivotedQuerySynthesizer;
import sqlancer.sqlite3.queries.SQLite3QueryPartitioningAggregateTester;
import sqlancer.sqlite3.queries.SQLite3QueryPartitioningDistinctTester;
import sqlancer.sqlite3.queries.SQLite3QueryPartitioningHavingTester;
import sqlancer.sqlite3.queries.SQLite3QueryPartitioningWhereTester;

@Parameters(separators = "=", commandDescription = "SQLite3")
public class SQLite3Options {

	@Parameter(names = { "--test-fts" }, description = "Test the FTS extensions", arity = 1)
	public boolean testFts = true;

	@Parameter(names = { "--test-rtree" }, description = "Test the R*Tree extensions", arity = 1)
	public boolean testRtree = true;

	@Parameter(names = { "--test-dbstats" }, description = "Test the DBSTAT Virtual Table (see https://www.sqlite.org/dbstat.html)", arity = 1)
	public boolean testDBStats = true;
	
	@Parameter(names = { "--test-generated-columns" }, description = "Test generated columns", arity = 1)
	public boolean testGeneratedColumns = true;
	
	@Parameter(names = { "--test-foreign-keys" }, description = "Test foreign key constraints", arity = 1)
	public boolean testForeignKeys = true;
	
	@Parameter(names = { "--test-without-rowids" }, description = "Generate WITHOUT ROWID tables", arity = 1)
	public boolean testWithoutRowids = true;

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public SQLite3Oracle oracle = SQLite3Oracle.METAMORPHIC;

	public static enum SQLite3Oracle {
		PQS() {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3PivotedQuerySynthesizer(globalState);
			}
		},
		METAMORPHIC() {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3MetamorphicQuerySynthesizer(globalState);
			}
		},
		FUZZER() {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3Fuzzer(globalState);
			}
		},
		AGGREGATE() {

			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3QueryPartitioningAggregateTester(globalState);
			}

		},
		WHERE {

			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3QueryPartitioningWhereTester(globalState);
			}
			
		},
		DISTINCT {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3QueryPartitioningDistinctTester(globalState);
			}
		},
		HAVING() {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				return new SQLite3QueryPartitioningHavingTester(globalState);
			}
		},
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
				List<TestOracle> oracles = new ArrayList<>();
				oracles.add(new SQLite3QueryPartitioningAggregateTester(globalState));
				oracles.add(new SQLite3QueryPartitioningHavingTester(globalState));
				return new CompositeTestOracle(oracles);
			}
		};

		public abstract TestOracle create(SQLite3GlobalState globalState) throws SQLException;

	}

	public class SQLite3OracleConverter implements IStringConverter<SQLite3Oracle> {
		@Override
		public SQLite3Oracle convert(String value) {
			return SQLite3Oracle.valueOf(value);
		}
	}

}
