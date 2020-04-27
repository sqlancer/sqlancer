package sqlancer.sqlite3;

import java.io.File;

public class SQLite3GeneratorOptions {
	
	public String databaseFile = "." + File.separator + "databases/test.db";
	
	public boolean deleteIfExists = true;

	public boolean generateDatabase = true;

	public int printNrQueries = 1000;

}
