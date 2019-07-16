package lama;

import com.beust.jcommander.Parameter;

public class MainOptions {

	@Parameter(names = {
			"--nrthreads-sqlite3" }, description = "How many SQLite3 threads should be started concurrently")
	private int nrSQLite3Threads = 16;

	@Parameter(names = { "--nrthreads-mysql" }, description = "How many MySQL threads should be started concurrently")
	private int nrMySQLThreads = 16;
	
	@Parameter(names = { "--nrthreads-postgres" }, description = "How many PostgreSQL threads should be started concurrently")
	private int nrPostgresThreads = 16;

	@Parameter(names = { "--nrtries" }, description = "Specifies after how many found errors to give up")
	private int totalNumberTries = 100;

	@Parameter(names = "--log-each-select", description = "Logs every statement issued")
	private boolean logEachSelect = true;
	
	@Parameter(names = "--sqliteOptions")
	private String sqliteOptions;

	public int getTotalNumberTries() {
		return totalNumberTries;
	}

	public int getNumberConcurrentThreads() {
		return nrSQLite3Threads + nrMySQLThreads + nrPostgresThreads;
	}

	public int getTotalNumberMysqlThreads() {
		return nrMySQLThreads;
	}
	
	public int getTotalNumberPostgresThreads() {
		return nrPostgresThreads;
	}

	public int getTotalNumberSQLite3Threads() {
		return nrSQLite3Threads;
	}

	public boolean logEachSelect() {
		return logEachSelect;
	}
	
	public String getSQLite3Options() {
		return sqliteOptions;
	}

}
