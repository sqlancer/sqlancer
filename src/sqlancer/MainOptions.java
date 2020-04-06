package sqlancer;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=", commandDescription = "Options applicable to all DBMS")
public class MainOptions {

	@Parameter(names = {
			"--num_threads" }, description = "How many threads should run concurrently to test separate databases")
	private int nrConcurrentThreads = 16;

	@Parameter(names = { "--num_tries" }, description = "Specifies after how many found errors to stop testing")
	private int totalNumberTries = 100;

	@Parameter(names = { "--max_num_inserts" }, description = "Specifies how many INSERT statements should be issued")
	private int maxNumberInserts = 30;

	@Parameter(names = {
			"--max_expression_depth" }, description = "Specifies the maximum depth of randomly-generated expressions")
	private int maxExpressionDepth = 3;

	@Parameter(names = {
			"--num_queries" }, description = "Specifies the number of queries to be issued to a database before creating a new database")
	private int nrQueries = 100000;

	@Parameter(names = {
			"--num_statement_kind_retries" }, description = "Specifies the number of times a specific statement kind (e.g., INSERT) should be retried when the DBMS indicates that it failed")
	private int nrStatementRetryCount = 1000;

	@Parameter(names = "--log-each-select", description = "Logs every statement issued", arity = 1)
	private boolean logEachSelect = true;
	
	@Parameter(names="--username", description = "The user name used to log into the DBMS")
	private String userName = "sqlancer";
	
	@Parameter(names="--password", description = "The password used to log into the DBMS")
	private String password = "sqlancer";
	
	public int getMaxExpressionDepth() {
		return maxExpressionDepth;
	}

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

	public int getNrQueries() {
		return nrQueries;
	}

	public int getMaxNumberInserts() {
		return maxNumberInserts;
	}
	
	public int getNrStatementRetryCount() {
		return nrStatementRetryCount;
	}

	public static enum DBMS {
		MariaDB, SQLite3, MySQL, PostgreSQL, TDEngine, CockroachDB, TiDB
	}
	
	public String getUserName() {
		return userName;
	}
	
	public String getPassword() {
		return password;
	}

	public class DBMSConverter implements IStringConverter<DBMS> {
		@Override
		public DBMS convert(String value) {
			return DBMS.valueOf(value);
		}
	}

}
