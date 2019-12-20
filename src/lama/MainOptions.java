package lama;

import com.beust.jcommander.Parameter;

public class MainOptions {

	@Parameter(names = {
			"--num_threads" }, description = "How many threads should run concurrently to test separate databases")
	private int nrConcurrentThreads = 16;

	@Parameter(names = { "--num_tries" }, description = "Specifies after how many found errors to stop testing")
	private int totalNumberTries = 100;

	@Parameter(names = "--log-each-select", description = "Logs every statement issued")
	private boolean logEachSelect = true;

	@Parameter(names = "--sqliteOptions")
	private String sqliteOptions;

	public int getTotalNumberTries() {
		return totalNumberTries;
	}

	public int getNumberConcurrentThreads() {
		return nrConcurrentThreads;
	}

	public int getTotalNumberSQLite3Threads() {
		return nrConcurrentThreads;
	}

	public boolean logEachSelect() {
		return logEachSelect;
	}

	public String getSQLite3Options() {
		return sqliteOptions;
	}

}
