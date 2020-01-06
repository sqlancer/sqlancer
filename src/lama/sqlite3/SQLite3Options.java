package lama.sqlite3;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import lama.MainOptions.DBMSConverter;

public class SQLite3Options {

	@Parameter(names = { "--test-fts" }, description = "Test the FTS extensions", arity = 1)
	public boolean testFts = true;

	@Parameter(names = { "--test-rtree" }, description = "Test the R*Tree extensions", arity = 1)
	public boolean testRtree = true;

	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public SQLite3Oracle oracle = SQLite3Oracle.METAMORPHIC;

	public static enum SQLite3Oracle {
		PQS, METAMORPHIC
	}

	public class SQLite3OracleConverter implements IStringConverter<SQLite3Oracle> {
		@Override
		public SQLite3Oracle convert(String value) {
			return SQLite3Oracle.valueOf(value);
		}
	}

}
