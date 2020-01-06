package lama.sqlite3;

import com.beust.jcommander.Parameter;

public class SQLite3Options {

	@Parameter(names = { "--test-fts" }, description = "Test the FTS extensions", arity = 1)
	public boolean testFts = true;

	@Parameter(names = { "--test-rtree" }, description = "Test the R*Tree extensions", arity = 1)
	public boolean testRtree = true;

}
