package lama;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import org.sqlite.SQLiteException;

import lama.Expression.Constant;
import lama.Main.StateToReproduce.ErrorKind;
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Helper;
import lama.tablegen.sqlite3.SQLite3AlterTable;
import lama.tablegen.sqlite3.SQLite3AnalyzeGenerator;
import lama.tablegen.sqlite3.SQLite3Common;
import lama.tablegen.sqlite3.SQLite3DeleteGenerator;
import lama.tablegen.sqlite3.SQLite3DropIndexGenerator;
import lama.tablegen.sqlite3.SQLite3IndexGenerator;
import lama.tablegen.sqlite3.SQLite3PragmaGenerator;
import lama.tablegen.sqlite3.SQLite3ReindexGenerator;
import lama.tablegen.sqlite3.SQLite3RowGenerator;
import lama.tablegen.sqlite3.SQLite3TableGenerator;
import lama.tablegen.sqlite3.SQLite3TransactionGenerator;
import lama.tablegen.sqlite3.SQLite3UpdateGenerator;
import lama.tablegen.sqlite3.SQLite3VacuumGenerator;

// TODO:
// group by
// case
// between
// select so that no record should be returned
// insert with arbitrary expressions
// views
// alter table
// triggers
public class Main {

	private static final int NR_QUERIES_PER_TABLE = 10000;
	private static final int MAX_INSERT_ROW_TRIES = 1;
	private static final int TOTAL_NR_THREADS = 100;
	private static final int NR_CONCURRENT_THREADS = 8;
	public static final int NR_INSERT_ROW_TRIES = 300;
	public static final int EXPRESSION_MAX_DEPTH = 3;
	public static final File LOG_DIRECTORY = new File("logs");
	static volatile AtomicLong nrQueries = new AtomicLong();

	public static class ReduceMeException extends RuntimeException {

		private static final long serialVersionUID = -3701934543692760005L;

	}

	public final static class StateToReproduce {

		public enum ErrorKind {
			EXCEPTION, ROW_NOT_FOUND
		}

		private ErrorKind errorKind;

		private final List<Query> statements = new ArrayList<>();
		public String query;

		private String databaseName;
		public Map<Column, Constant> randomRowValues;

		private Logger logger;
		protected String databaseVersion;

		public String values;

		private String exception;

		public String queryTargetedTablesString;

		protected String queryTargetedColumnsString;

		public StateToReproduce(String databaseName) {
			this.databaseName = databaseName;

		}

		public Logger getLogger() {
			if (logger == null) {
				try {
					FileHandler fh = new FileHandler(new File(LOG_DIRECTORY, databaseName + ".log").getAbsolutePath());
					fh.setFormatter(new SimpleFormatter());
					logger = Logger.getLogger(databaseName);
					logger.addHandler(fh);
				} catch (Exception e) {
					throw new AssertionError(e);
				}
			}
			assert logger != null;
			return logger;
		}

		public String logInconsistency(Throwable e1) {
			this.exception = e1.getMessage();
			String stackTrace = getStackTrace(e1);
			getLogger().severe(stackTrace);
			getLogger().severe(toString());
			this.errorKind = ErrorKind.EXCEPTION;
			return toString();
		}

		public String getException() {
			assert this.errorKind == ErrorKind.EXCEPTION;
			return exception;
		}

		private String getStackTrace(Throwable e1) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e1.printStackTrace(pw);
			String stackTrace = sw.toString();
			return stackTrace;
		}

		public String logInconsistency() {
			getLogger().severe(toString());
			this.errorKind = ErrorKind.ROW_NOT_FOUND;
			return toString();
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
			if (query != null) {
				sb.append(query + ";\n");
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
		PRAGMA, INDEX, INSERT, VACUUM, REINDEX, ANALYZE, DELETE, TRANSACTION_START, ALTER, DROP_INDEX, UPDATE;
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

				@Override
				public void run() {
					runThread(databaseName);
				}

				private void runThread(final String databaseName) {
					Thread.currentThread().setName(databaseName);
					while (true) {
						try (Connection con = DatabaseFacade.createDatabase(databaseName)) {
							generateAndTestDatabase(databaseName, con);
						} catch (ReduceMeException reduce) {
							state.logInconsistency();
							tryToReduceBug(databaseName, state);
							break;
						} catch (Exception reduce) {
							Thread.currentThread().setName(databaseName + " (reducing)");
							state.logInconsistency(reduce);
							tryToReduceBug(databaseName, state);
							break;
						} catch (Throwable e1) {
							e1.printStackTrace();
							state.logInconsistency(e1);
							threadsShutdown++;
							break;
						}

					}
				}

				private void generateAndTestDatabase(final String databaseName, Connection con) throws SQLException {
					state = new StateToReproduce(databaseName);
					Schema newSchema = null;
					int nrTablesToCreate = 1 + Randomly.smallNumber();
					for (int i = 0; i < nrTablesToCreate; i++) {
						newSchema = Schema.fromConnection(con);
						String tableName = SQLite3Common.createTableName(i);
						Query tableQuery = SQLite3TableGenerator.createTableStatement(tableName, state, newSchema);
						state.statements.add(tableQuery);
						tableQuery.execute(con);
					}

					newSchema = Schema.fromConnection(con);

					java.sql.DatabaseMetaData meta = con.getMetaData();
					state.databaseVersion = meta.getDatabaseProductVersion();

					int[] nrRemaining = new int[Action.values().length];
					List<Action> actions = new ArrayList<>();
					int total = 0;
					for (int i = 0; i < Action.values().length; i++) {
						Action action = Action.values()[i];
						int nrPerformed = 0;
						switch (action) {
						case INDEX:
						case PRAGMA:
							nrPerformed = Randomly.getInteger(5, 100);
							break;
						case REINDEX:
						case VACUUM:
						case ANALYZE:
						case DROP_INDEX:
						case TRANSACTION_START:
							nrPerformed = Randomly.smallNumber();
							break;
						case INSERT:
							nrPerformed = NR_INSERT_ROW_TRIES;
							break;
						case DELETE:
							nrPerformed = Randomly.getBoolean() ? 1 : 0;
							break;
						case ALTER:
							nrPerformed = Randomly.smallNumber();
							break;
						default:
							throw new AssertionError();
						}
						if (nrPerformed != 0) {
							actions.add(action);
						}
						nrRemaining[action.ordinal()] = nrPerformed;
						total += nrPerformed;
					}

					boolean transactionActive = false;
					while (total != 0) {
						Action nextAction = null;
						int selection = Randomly.getInteger(0, total);
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
						boolean affectedSchema = false;
						switch (nextAction) {
						case ALTER:
							query = SQLite3AlterTable.alterTable(newSchema, con, state);
							affectedSchema = true;
							break;
						case UPDATE:
							query = SQLite3UpdateGenerator.updateRow(newSchema.getRandomTable(), con, state);
							break;
						case TRANSACTION_START:
							if (!transactionActive) {
								SQLite3TransactionGenerator.generateBeginTransaction(con, state);
								transactionActive = true;
							} else {
								query = SQLite3TransactionGenerator.generateCommit(con, state);
								transactionActive = false;
							}
						case INDEX:
							query = SQLite3IndexGenerator.insertIndex(con, state);
							state.statements.add(query);
							query.execute(con);
							transactionActive = false;
							query = SQLite3TransactionGenerator.generateCommit(con, state);
							break;
						case DROP_INDEX:
							query = SQLite3DropIndexGenerator.dropIndex(con, state, newSchema);
							break;
						case DELETE:
							query = SQLite3DeleteGenerator
									.deleteContent(Randomly.fromList(newSchema.getDatabaseTables()), con, state);
							break;
						case INSERT:
							Table randomTable = Randomly.fromList(newSchema.getDatabaseTables());
							query = SQLite3RowGenerator.insertRow(randomTable, con, state);
							break;
						case PRAGMA:
							query = SQLite3PragmaGenerator.insertPragma(con, state);
							break;
						case REINDEX:
							query = SQLite3ReindexGenerator.executeReindex(con, state);
							break;
						case VACUUM:
							if (transactionActive) {
								query = SQLite3TransactionGenerator.generateCommit(con, state);
								query.execute(con);
								transactionActive = false;
							}
							query = SQLite3VacuumGenerator.executeVacuum(con, state);
							break;
						case ANALYZE:
							query = SQLite3AnalyzeGenerator.generateAnalyze();
							break;
						default:
							throw new AssertionError(nextAction);
						}
						state.statements.add(query);
						query.execute(con);
						total--;
						if (affectedSchema) {
							newSchema = Schema.fromConnection(con);
						}
					}
					for (Table t : newSchema.getDatabaseTables()) {
						if (!ensureTableHasRows(con, t)) {
							return;
						}
					}
					if (Randomly.getBoolean()) {
						SQLite3ReindexGenerator.executeReindex(con, state);
					}
					if (transactionActive) {
						SQLite3TransactionGenerator.generateCommit(con, state);
						transactionActive = false;
					}
					QueryGenerator queryGenerator = new QueryGenerator(con);
					for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
						queryGenerator.generateAndCheckQuery(state);
						nrQueries.addAndGet(1);
					}
				}

				private boolean ensureTableHasRows(Connection con, Table randomTable)
						throws AssertionError, SQLException {
					int nrRows;
					int counter = MAX_INSERT_ROW_TRIES;
					do {
						try {
							Query q = SQLite3RowGenerator.insertRow(randomTable, con, state);
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
					threadsShutdown++;
					List<Query> reducedStatements = new ArrayList<>(state.statements);
					Query statementThatCausedException = state.statements.get(state.statements.size() - 1);
					if (state.getErrorKind() == ErrorKind.EXCEPTION) {
						reducedStatements.remove(statementThatCausedException);
					}
					retry: for (int i = 0; i < 10; i++) {
						List<Query> currentRoundReducedStatements = new ArrayList<>(reducedStatements);
						if (currentRoundReducedStatements.isEmpty()) {
							break;
						}
						int nrToRemove = currentRoundReducedStatements.size() / 20;
						for (int j = 0; j < nrToRemove; j++) {
							Query q = Randomly.fromList(currentRoundReducedStatements);
							currentRoundReducedStatements.remove(q);
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
									try (ResultSet result = s.executeQuery(state.query)) {
										boolean isContainedIn = !result.isClosed();
										if (isContainedIn) {
											continue retry;
										} else {
											String checkRowIsInside = "SELECT " + state.queryTargetedColumnsString + " FROM " + state.queryTargetedTablesString
													+ " INTERSECT SELECT " + state.values;
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
						} catch (SQLException e) {
							throw new AssertionError(e);
						}
					}

					int fromSize = state.statements.size();
					int toSize = reducedStatements.size();
					state.statements.clear();
					state.statements.add(new QueryAdapter("-- reduced from " + fromSize + " " + toSize));
					state.statements.addAll(reducedStatements);
					if (state.getErrorKind() == ErrorKind.EXCEPTION) {
						state.statements.add(statementThatCausedException);
					}
					state.logInconsistency();
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
