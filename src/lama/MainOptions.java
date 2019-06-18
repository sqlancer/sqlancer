package lama;

import com.beust.jcommander.Parameter;

public class MainOptions {

	@Parameter(names = {
			"--nrthreads-sqlite3" }, description = "How many SQLite3 threads should be started concurrently")
	private int nrSQLite3Threads = 16;

	@Parameter(names = { "--nrthreads-mysql" }, description = "How many MySQL threads should be started concurrently")
	private int nrMySQLThreads = 16;

	@Parameter(names = { "--nrtries" }, description = "Specifies after how many found errors to give up")
	private int totalNumberTries = 100;

	public int getTotalNumberTries() {
		return totalNumberTries;
	}

	public int getNumberConcurrentThreads() {
		return nrSQLite3Threads + nrMySQLThreads;
	}

	public int getTotalNumberMysqlThreads() {
		return nrMySQLThreads;
	}

	public int getTotalNumberSQLite3Threads() {
		return nrSQLite3Threads;
	}

}
