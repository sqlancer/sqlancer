package lama;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import lama.Main.StateToReproduce.ErrorKind;
import lama.sqlite3.SQLite3Helper;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3AlterTable;
import lama.sqlite3.gen.SQLite3AnalyzeGenerator;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3DeleteGenerator;
import lama.sqlite3.gen.SQLite3DropIndexGenerator;
import lama.sqlite3.gen.SQLite3IndexGenerator;
import lama.sqlite3.gen.SQLite3PragmaGenerator;
import lama.sqlite3.gen.SQLite3ReindexGenerator;
import lama.sqlite3.gen.SQLite3RowGenerator;
import lama.sqlite3.gen.SQLite3TableGenerator;
import lama.sqlite3.gen.SQLite3TransactionGenerator;
import lama.sqlite3.gen.SQLite3UpdateGenerator;
import lama.sqlite3.gen.SQLite3VacuumGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

// TODO:
// group by
// case
// between
// select so that no record should be returned
// insert with arbitrary expressions
// views
// alter table
// triggers
// MIN/MAX
// as
// NaN values
public class Main {

	private static final int NR_QUERIES_PER_TABLE = 1000;
	private static final int MAX_INSERT_ROW_TRIES = 1;
	private static final int TOTAL_NR_THREADS = 100;
	private static final int NR_CONCURRENT_THREADS = 16;
	public static final int NR_INSERT_ROW_TRIES = 10;
	public static final int EXPRESSION_MAX_DEPTH = 3;
	public static final File LOG_DIRECTORY = new File("logs");
	static volatile AtomicLong nrQueries = new AtomicLong();

	public static class ReduceMeException extends RuntimeException {

		private static final long serialVersionUID = -3701934543692760005L;

	}

	public final static class StateLogger {

		private final File loggerFile;
		private final File reducedFile;
		private File curFile;
		private FileWriter logFileWriter;
		private FileWriter reducedFileWriter;
		private FileWriter currentFileWriter;

		private final static class AlsoWriteToConsoleFileWriter extends FileWriter {

			public AlsoWriteToConsoleFileWriter(File file) throws IOException {
				super(file);
			}

			@Override
			public Writer append(CharSequence arg0) throws IOException {
				System.err.println(arg0);
				return super.append(arg0);
			}

			@Override
			public void write(String str) throws IOException {
				System.err.println(str);
				super.write(str);
			}
		}

		public StateLogger(String databaseName) {
			loggerFile = new File(LOG_DIRECTORY, databaseName + ".log");
			reducedFile = new File(LOG_DIRECTORY, databaseName + "-reduced.log");
			curFile = new File(LOG_DIRECTORY, databaseName + "-cur.log");
		}

		private FileWriter getLogFileWriter() {
			if (logFileWriter == null) {
				try {
					logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}
			return logFileWriter;
		}

		private FileWriter getReducedFileWriter() {
			if (reducedFileWriter == null) {
				try {
					reducedFileWriter = new FileWriter(reducedFile);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}
			return reducedFileWriter;
		}

		private FileWriter getCurrentFileWriter() {
			if (currentFileWriter == null) {
				try {
					currentFileWriter = new FileWriter(curFile, false);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}
			return currentFileWriter;
		}

		public void writeCurrent(StateToReproduce state) {
			printState(getCurrentFileWriter(), state);
			try {
				currentFileWriter.flush();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void writeCurrent(String queryString) {
			try {
				getCurrentFileWriter().write(queryString + ";\n");
				currentFileWriter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void logRowNotFound(StateToReproduce state) {
			printState(getLogFileWriter(), state);
			try {
				getLogFileWriter().flush();
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}

		public void logException(Throwable reduce, StateToReproduce state) {
			String stackTrace = getStackTrace(reduce);
			try {
				FileWriter logFileWriter2 = getLogFileWriter();
				logFileWriter2.write(stackTrace);
				printState(logFileWriter2, state);
				logFileWriter2.flush();
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}

		private String getStackTrace(Throwable e1) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e1.printStackTrace(pw);
			String stackTrace = sw.toString().replace("\n", "\n--");
			return stackTrace;
		}

		public void logReduced(StateToReproduce state, int i, int from, int to) {
			try {
				FileWriter fw = getReducedFileWriter();
				fw.append("-- Reduce run nr " + i + "\n");
				fw.append("-- Reduced from " + from + " to " + to + "\n");
				printState(fw, state);
				reducedFileWriter.close();
				reducedFileWriter = null;
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}

		private void printState(FileWriter writer, StateToReproduce state) {
			StringBuilder sb = new StringBuilder();
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			sb.append("-- Time: " + dateFormat.format(date) + "\n");
			sb.append("-- Database: " + state.getDatabaseName() + "\n");
			sb.append("-- SQLite3 version: " + state.getDatabaseVersion() + "\n");
			for (Query s : state.getStatements()) {
				if (s.getQueryString().endsWith(";")) {
					sb.append(s.getQueryString());
				} else {
					sb.append(s.getQueryString() + ";");
				}
				sb.append('\n');
			}
			if (state.getQueryString() != null) {
				sb.append(state.getQueryString() + ";\n");
			}
			if (state.getRandomRowValues() != null) {
				List<Column> columnList = state.getRandomRowValues().keySet().stream().collect(Collectors.toList());
				List<Table> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
						.collect(Collectors.toList());
				for (Table t : tableList) {
					sb.append("-- " + t.getName() + "\n");
					List<Column> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
							.collect(Collectors.toList());
					for (Column c : columnsForTable) {
						sb.append("--\t");
						sb.append(c);
						sb.append("=");
						sb.append(state.getRandomRowValues().get(c));
						sb.append("\n");
					}
				}
				sb.append("expected values: \n");
				sb.append(SQLite3Visitor.asExpectedValues(state.getWhereClause()));
			}
			try {
				writer.write(sb.toString());
				writer.flush();
			} catch (IOException e) {
				throw new AssertionError();
			}
		}

	}

	public final static class StateToReproduce {

		public enum ErrorKind {
			EXCEPTION, ROW_NOT_FOUND
		}

		private ErrorKind errorKind;

		private final List<Query> statements = new ArrayList<>();
		public String queryString;

		private String databaseName;
		public Map<Column, SQLite3Constant> randomRowValues;

		protected String databaseVersion;

		public String values;

		private String exception;

		public String queryTargetedTablesString;

		public String queryTargetedColumnsString;

		public SQLite3Expression whereClause;
		
		public StateToReproduce(String databaseName) {
			this.databaseName = databaseName;

		}

		public String getException() {
			assert this.errorKind == ErrorKind.EXCEPTION;
			return exception;
		}

		public String getDatabaseName() {
			return databaseName;
		}

		public String getDatabaseVersion() {
			return databaseVersion;
		}

		public List<Query> getStatements() {
			return statements;
		}

		public String getQueryString() {
			return queryString;
		}
		
		public SQLite3Expression getWhereClause() {
			return whereClause;
		}

		public Map<Column, SQLite3Constant> getRandomRowValues() {
			return randomRowValues;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("database: " + databaseName + "\n");
			sb.append("version: " + databaseVersion + "\n");
			if (randomRowValues != null) {
				sb.append("expected values: " + randomRowValues + "\n");
			}
			sb.append("-- statements start here\n");
			if (statements != null) {
				sb.append(statements.stream().map(q -> q.getQueryString()).collect(Collectors.joining(";\n")) + "\n");
			}
			if (queryString != null) {
				sb.append(queryString + ";\n");
			}
			return sb.toString();
		}

		public ErrorKind getErrorKind() {
			return errorKind;
		}

		public void setErrorKind(ErrorKind errorKind) {
			this.errorKind = errorKind;
		}

	}

	static int threadsShutdown;

	private enum Action {
		PRAGMA, INDEX, INSERT, VACUUM, REINDEX, ANALYZE, DELETE, TRANSACTION_START, ALTER, DROP_INDEX, UPDATE,
		ROLLBACK_TRANSACTION, COMMIT;
	}

	public static void main(String[] args) {

		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(new Runnable() {

			private long timeMillis = System.currentTimeMillis();
			private long lastNrQueries = 0;

			{
				timeMillis = System.currentTimeMillis();
			}

			@Override
			public void run() {
				long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
				long currentNrQueries = nrQueries.get();
				long nrCurrentQueries = currentNrQueries - lastNrQueries;
				double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				System.out.println(String.format("[%s] Executed %d queries (%d queries/s). Threads shut down: %d.",
						dateFormat.format(date), currentNrQueries, (int) throughput, threadsShutdown));
				timeMillis = System.currentTimeMillis();
				lastNrQueries = currentNrQueries;
			}
		}, 5, 5, TimeUnit.SECONDS);

		setupLogDirectory();

		ExecutorService executor = Executors.newFixedThreadPool(NR_CONCURRENT_THREADS);

		for (int i = 0; i < TOTAL_NR_THREADS; i++) {
			final String databaseName = "lama" + i;
			executor.execute(new Runnable() {

				StateToReproduce state;
				StateLogger logger = new StateLogger(databaseName);

				@Override
				public void run() {
					runThread(databaseName);
				}

				private void runThread(final String databaseName) {
					Thread.currentThread().setName(databaseName);
					while (true) {
						try (Connection con = DatabaseFacade.createDatabase(databaseName)) {
							generateAndTestDatabase(databaseName, con);
						} catch (IgnoreMeException e) {
							continue;
						} catch (ReduceMeException reduce) {
							state.errorKind = ErrorKind.ROW_NOT_FOUND;
							logger.logRowNotFound(state);
							tryToReduceBug(databaseName, state);
							threadsShutdown++;

							break;
						} catch (Throwable reduce) {
							reduce.printStackTrace();
							state.errorKind = ErrorKind.EXCEPTION;
							state.exception = reduce.getMessage();
							logger.logException(reduce, state);
							tryToReduceBug(databaseName, state);
							threadsShutdown++;
							break;
						} finally {
							try {
								if (logger.currentFileWriter != null)
									logger.currentFileWriter.close();
								logger.currentFileWriter = null;
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}

				private void generateAndTestDatabase(final String databaseName, Connection con) throws SQLException {
					Randomly r = new Randomly();
					state = new StateToReproduce(databaseName);
					SQLite3Schema newSchema = null;

					addSensiblePragmaDefaults(con);
					int nrTablesToCreate = 1 + Randomly.smallNumber();
					for (int i = 0; i < nrTablesToCreate; i++) {
						newSchema = SQLite3Schema.fromConnection(con);
						assert newSchema.getDatabaseTables().size() == i : newSchema + " " + i;
						String tableName = SQLite3Common.createTableName(i);
						Query tableQuery = SQLite3TableGenerator.createTableStatement(tableName, state, newSchema, r);
						state.statements.add(tableQuery);
						tableQuery.execute(con);
					}

					newSchema = SQLite3Schema.fromConnection(con);

					java.sql.DatabaseMetaData meta = con.getMetaData();
					state.databaseVersion = meta.getDatabaseProductVersion();

					int[] nrRemaining = new int[Action.values().length];
					List<Action> actions = new ArrayList<>();
					int total = 0;
					for (int i = 0; i < Action.values().length; i++) {
						Action action = Action.values()[i];
						int nrPerformed = 0;
						switch (action) {
						case ALTER:
							nrPerformed = r.getInteger(0, 5);
							break;
						case INSERT:
							nrPerformed = NR_INSERT_ROW_TRIES;
							break;
						case COMMIT:
						case TRANSACTION_START:
						case INDEX:
						case REINDEX:
						case VACUUM:
						case UPDATE:
						case ANALYZE:
						case PRAGMA:
						case DROP_INDEX:
						case DELETE:
						case ROLLBACK_TRANSACTION:
						default:
							nrPerformed = r.getInteger(1, 30);
							break;
						}
						if (nrPerformed != 0) {
							actions.add(action);
						}
						nrRemaining[action.ordinal()] = nrPerformed;
						total += nrPerformed;
					}

					while (total != 0) {
						Action nextAction = null;
						int selection = r.getInteger(0, total);
						int previousRange = 0;
						for (int i = 0; i < nrRemaining.length; i++) {
							if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
								nextAction = Action.values()[i];
								break;
							} else {
								previousRange += nrRemaining[i];
							}
						}
						assert nextAction != null;
						assert nrRemaining[nextAction.ordinal()] > 0;
						nrRemaining[nextAction.ordinal()]--;
						Query query;
						switch (nextAction) {
						case ALTER:
							query = SQLite3AlterTable.alterTable(newSchema, con, state, r);
							break;
						case ROLLBACK_TRANSACTION:
							query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
							break;
						case UPDATE:
							query = SQLite3UpdateGenerator.updateRow(newSchema.getRandomTable(), con, state, r);
							break;
						case TRANSACTION_START:
							query = SQLite3TransactionGenerator.generateBeginTransaction(con, state);
							break;
						case INDEX:
							query = SQLite3IndexGenerator.insertIndex(con, state, r);
							break;
						case DROP_INDEX:
							query = SQLite3DropIndexGenerator.dropIndex(con, state, newSchema, r);
							break;
						case DELETE:
							query = SQLite3DeleteGenerator
									.deleteContent(Randomly.fromList(newSchema.getDatabaseTables()), con, state, r);
							break;
						case INSERT:
							Table randomTable = Randomly.fromList(newSchema.getDatabaseTables());
							query = SQLite3RowGenerator.insertRow(randomTable, con, state, r);
							break;
						case PRAGMA:
							query = SQLite3PragmaGenerator.insertPragma(con, state, r);
							break;
						case REINDEX:
							query = SQLite3ReindexGenerator.executeReindex(con, state);
							break;
						case COMMIT:
							query = SQLite3TransactionGenerator.generateCommit(con, state);
							break;
						case VACUUM:
							query = SQLite3VacuumGenerator.executeVacuum(con, state);
							break;
						case ANALYZE:
							query = SQLite3AnalyzeGenerator.generateAnalyze();
							break;
						default:
							throw new AssertionError(nextAction);
						}
						state.statements.add(query);
						try {
							query.execute(con);
							if (query.couldAffectSchema()) {
								newSchema = SQLite3Schema.fromConnection(con);
							}
						} catch (Throwable t) {
							System.err.println(query.getQueryString());
							throw t;
						}
						total--;
					}
					Query query = SQLite3TransactionGenerator.generateCommit(con, state);
					query.execute(con);
					// also do an abort for DEFERRABLE INITIALLY DEFERRED
					state.statements.add(query);

					query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
					query.execute(con);
					state.statements.add(query);
					newSchema = SQLite3Schema.fromConnection(con);

					for (Table t : newSchema.getDatabaseTables()) {
						if (!ensureTableHasRows(con, t, r)) {
							return;
						}
					}
					if (Randomly.getBoolean()) {
						SQLite3ReindexGenerator.executeReindex(con, state);
					}
					newSchema = SQLite3Schema.fromConnection(con);

					// logger.writeCurrent(state);
					QueryGenerator queryGenerator = new QueryGenerator(con, r);
					for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
						queryGenerator.generateAndCheckQuery(state, logger);
						nrQueries.addAndGet(1);
					}
				}

				private void addSensiblePragmaDefaults(Connection con) throws SQLException {
					List<String> defaultSettings = Arrays.asList("PRAGMA cache_size = 10000;",
							"PRAGMA temp_store=MEMORY;", "PRAGMA synchronous=off;");
					for (String s : defaultSettings) {
						Query q = new QueryAdapter(s);
						state.statements.add(q);
						q.execute(con);
					}
				}
				
				private boolean ensureTableHasRows(Connection con, Table randomTable, Randomly r)
						throws AssertionError, SQLException {
					int nrRows;
					int counter = MAX_INSERT_ROW_TRIES;
					do {
						try {
							Query q = SQLite3RowGenerator.insertRow(randomTable, con, state, r);
							state.statements.add(q);
							q.execute(con);

						} catch (SQLException e) {
							if (!QueryGenerator.shouldIgnoreException(e)) {
								throw new AssertionError(e);
							}
						}
						nrRows = SQLite3Helper.getNrRows(con, randomTable);
					} while (nrRows == 0 && counter-- != 0);
					return nrRows != 0;
				}

				private void tryToReduceBug(final String databaseName, StateToReproduce state) {
					List<Query> reducedStatements = new ArrayList<>(state.statements);
					Query statementThatCausedException = state.statements.get(state.statements.size() - 1);
					int initialStatementNr = state.statements.size();
					if (state.getErrorKind() == ErrorKind.EXCEPTION) {
						reducedStatements.remove(statementThatCausedException);
					}
					retry: for (int i = 0; i < 1000; i++) {
						List<Query> currentRoundReducedStatements = new ArrayList<>(reducedStatements);
						if (currentRoundReducedStatements.isEmpty()) {
							break;
						}
						int nrToRemove;
						if (i == 0) {
							// first try to remove all statements that usually do not cause problems
							for (Query q : new ArrayList<>(currentRoundReducedStatements)) {
								String string = q.getQueryString();
								if (string.startsWith("PRAGMA") || string.startsWith("ANALYZE")
										|| string.startsWith("COMMIT") || string.startsWith("REINDEX")) {
									currentRoundReducedStatements.remove(q);
								}
							}
						} else {
							if (i >= 300) {
								nrToRemove = 1;
							} else if (i >= 100) {
								nrToRemove = currentRoundReducedStatements.size() / 20;
							} else {
								nrToRemove = currentRoundReducedStatements.size() / 10;
							}
							for (int j = 0; j < nrToRemove; j++) {
								Query q = Randomly.fromList(currentRoundReducedStatements);
								currentRoundReducedStatements.remove(q);
							}
						}

						try (Connection con = DatabaseFacade.createDatabase(databaseName + "_reduced")) {
							for (Query q : currentRoundReducedStatements) {
								try {
									q.execute(con);
								} catch (Throwable t) {
									continue retry; // unsuccessful;
								}
							}
							if (state.getErrorKind() == ErrorKind.EXCEPTION) {
								try {
									statementThatCausedException.execute(con);
								} catch (Throwable t) {
									if (!t.getMessage().equals(state.getException())) {
										continue retry; // unsuccessful
									}
								}
							} else {
								try (Statement s = con.createStatement()) {
									try (ResultSet result = s.executeQuery(state.queryString)) {
										boolean isContainedIn = !result.isClosed();
										if (isContainedIn) {
											continue retry;
										} else {
											String checkRowIsInside = "SELECT " + state.queryTargetedColumnsString
													+ " FROM " + state.queryTargetedTablesString + " INTERSECT SELECT "
													+ state.values;
											ResultSet result2 = s.executeQuery(checkRowIsInside);
											if (result2.isClosed()) {
												continue retry;
											}
											result.close();
										}
									}
								} catch (Throwable t) {
									continue retry;
								}
							}
							reducedStatements = currentRoundReducedStatements;
							state.statements.clear();
							state.statements.addAll(reducedStatements);
							if (state.getErrorKind() == ErrorKind.EXCEPTION) {
								state.statements.add(statementThatCausedException);
							}
							logger.logReduced(state, i, initialStatementNr, state.statements.size());
						} catch (SQLException e) {
							throw new AssertionError(e);
						}
					}

					return;
				}
			});
		}

	}

	private static void setupLogDirectory() {
		if (!LOG_DIRECTORY.exists()) {
			try {
				Files.createDirectories(LOG_DIRECTORY.toPath());
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}
		assert LOG_DIRECTORY.exists();
		for (File file : LOG_DIRECTORY.listFiles()) {
			if (!file.isDirectory()) {
				file.delete();
			}
		}
	}

}
