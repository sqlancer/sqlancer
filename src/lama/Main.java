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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import lama.Expression.Constant;
import lama.Main.StateToReproduce.ErrorKind;
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.tablegen.sqlite3.SQLite3IndexGenerator;
import lama.tablegen.sqlite3.SQLite3PragmaGenerator;
import lama.tablegen.sqlite3.SQLite3TableGenerator;
import lama.tablegen.sqlite3.Sqlite3RowGenerator;

public class Main {

	private static final int NR_QUERIES_PER_TABLE = 5000;
	private static final int TOTAL_NR_THREADS = 100;
	private static final int NR_CONCURRENT_THREADS = 1;
	public static final int NR_INSERT_ROW_TRIES = 50;
	public static final int EXPRESSION_MAX_DEPTH = 2;
	public static final File LOG_DIRECTORY = new File("logs");

	public static class ReduceMeException extends RuntimeException {

		private static final long serialVersionUID = -3701934543692760005L;

	}

	public final static class StateToReproduce {

		public enum ErrorKind {
			EXCEPTION, ROW_NOT_FOUND
		}

		private ErrorKind errorKind;

		public List<String> statements = new ArrayList<>();
		public String query;

		private String databaseName;
		public Map<Column, Constant> randomRowValues;

		private Logger logger;
		protected String databaseVersion;

		public String values;

		private String exception;

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
				sb.append(statements.stream().collect(Collectors.joining(";\n")) + "\n");
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
		PRAGMA, INDEX, INSERT, VACUUM, REINDEX, ANALYZE;
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
				long nrCurrentQueries = QueryGenerator.nrQueries - lastNrQueries;
				double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				System.out.println(String.format("[%s] Executed %d queries (%d queries/s). Threads shut down: %d.",
						dateFormat.format(date), QueryGenerator.nrQueries, (int) throughput, threadsShutdown));
				timeMillis = System.currentTimeMillis();
				lastNrQueries = QueryGenerator.nrQueries;
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
					// we try to reuse the same database since it greatly improves performance
					try (Connection con = DatabaseFacade.createDatabase(databaseName)) {
						while (true) {
							state = generateAndTestTable(databaseName, con);
						}
					} catch (ReduceMeException reduce) {
						tryToReduceBug(databaseName, state);
					} catch (Throwable e1) {
						state.logInconsistency(e1);
						threadsShutdown++;
						return;
					}
				}

				private StateToReproduce generateAndTestTable(final String databaseName, Connection con)
						throws SQLException {
					try (Statement s = con.createStatement()) {
						s.execute("DROP TABLE IF EXISTS test");
					}
					state = new StateToReproduce(databaseName);
					Statement createStatement = con.createStatement();
					String tableQuery = SQLite3TableGenerator.createTableStatement("test", state);
					createStatement.execute(tableQuery);
					createStatement.close();

					java.sql.DatabaseMetaData meta = con.getMetaData();
					state.databaseVersion = meta.getDatabaseProductVersion();

					int[] nrRemaining = new int[Action.values().length];
					List<Action> actions = new ArrayList<>();
					for (int i = 0; i < Action.values().length; i++) {
						Action action = Action.values()[i];
						int nrPerformed = 0;
						switch (action) {
						case INDEX:
						case PRAGMA:
						case REINDEX:
						case VACUUM:
						case ANALYZE:
							nrPerformed = Randomly.smallNumber();
							break;
						case INSERT:
							nrPerformed = NR_INSERT_ROW_TRIES;
							break;
						default:
							throw new AssertionError();
						}
						if (nrPerformed != 0) {
							actions.add(action);
						}
						nrRemaining[i] = nrPerformed;
					}
					while (!actions.isEmpty()) {
						Action nextAction = Randomly.fromList(actions);
						assert nrRemaining[nextAction.ordinal()] > 0;
						nrRemaining[nextAction.ordinal()]--;
						if (nrRemaining[nextAction.ordinal()] == 0) {
							boolean success = actions.remove(nextAction);
							assert success;
						}
						switch (nextAction) {
						case INDEX:
							try {
								SQLite3IndexGenerator.insertIndex(con, state);
							} catch (SQLException e) {
								// ignore
							}
							break;
						case INSERT:
							Sqlite3RowGenerator.insertRow(Schema.fromConnection(con).getRandomTable(), con, state);
							break;
						case PRAGMA:
							SQLite3PragmaGenerator.insertPragma(con, state, false);
							break;
						case REINDEX:
							try {
								if (Randomly.getBoolean()) {
									try (Statement s = con.createStatement()) {
										state.statements.add("REINDEX;");
										s.execute("REINDEX;");
									}
								}
							} catch (Throwable e) {
								state.logInconsistency(e);
								throw new ReduceMeException();
							}
							break;
						case VACUUM:
							try {
								if (Randomly.getBoolean()) {
									try (Statement s = con.createStatement()) {
										state.statements.add("VACUUM;");
										s.execute("VACUUM;");
									}
								}
							} catch (Throwable e) {
								state.logInconsistency(e);
								throw new ReduceMeException();
							}
							break;
						case ANALYZE:
							if (Randomly.getBoolean()) {
								try (Statement s = con.createStatement()) {
									state.statements.add("ANALYZE;");
									s.execute("ANALYZE;");
								}
							}
							break;
						default:
							throw new AssertionError(nextAction);
						}
					}
					int nrRows;
					do {
						Sqlite3RowGenerator.insertRow(Schema.fromConnection(con).getRandomTable(), con, state);
						nrRows = getNrRows(con);
					} while (nrRows == 0);
					QueryGenerator queryGenerator = new QueryGenerator(con);

					for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
						queryGenerator.generateAndCheckQuery(state);
					}
					return state;
				}

				private void tryToReduceBug(final String databaseName, StateToReproduce state) {
					threadsShutdown++;
					try (Connection con = DatabaseFacade.createDatabase(databaseName + "_reduced")) {
						try (Statement s = con.createStatement()) {
							s.execute("DROP TABLE IF EXISTS test");
						}
						List<String> reducedStatements = new ArrayList<>(state.statements);
						int i = 0;
						outer: while (i < reducedStatements.size()) {
							List<String> reducedTryStatements = new ArrayList<>(reducedStatements);
							String exceptionCausingStatement = state.statements.get(state.statements.size() - 1);
							if (state.getErrorKind() == ErrorKind.EXCEPTION
									&& reducedStatements.get(i).equals(exceptionCausingStatement)) {
								i++; // do not reduce the statement that caused the exception
								if (i >= reducedStatements.size()) {
									break;
								}
							}
							reducedTryStatements.remove(i);
							try (Statement statement = con.createStatement()) {
								for (int j = 0; j < reducedTryStatements.size(); j++) {
									String statementToExecute = reducedTryStatements.get(j);
									if (state.getErrorKind() == ErrorKind.EXCEPTION
											&& statementToExecute.equals(exceptionCausingStatement)) {
										try {
											statement.execute(statementToExecute);
										} catch (Throwable t) {
											assert reducedTryStatements.size() < reducedStatements.size();
											if (t.getMessage().equals(state.getException())) {
												reducedStatements = reducedTryStatements;
											}
										}
									} else {
										statement.execute(statementToExecute);
									}
								}
							} catch (Throwable t) {
								i++;
								continue outer;
							}
							if (state.getErrorKind() == ErrorKind.ROW_NOT_FOUND) {
								try (Statement s = con.createStatement()) {
									try {
										ResultSet result = s.executeQuery(state.query);
										boolean isContainedIn = !result.isClosed();
										if (!isContainedIn) {
											String checkRowIsInside = "SELECT * FROM test INTERSECT SELECT "
													+ state.values;
											ResultSet result2 = s.executeQuery(checkRowIsInside);
											if (!result2.isClosed()) {
												assert reducedTryStatements.size() < reducedStatements.size();
												reducedStatements = reducedTryStatements;
											} else {
												i++;
											}
										} else {
											i++;
										}
										result.close();
									} catch (Throwable t) {
										i++;
									}
								}
							}
						}
						state.statements = new ArrayList<>();
						state.statements.add("-- trying to reduce query");
						state.statements.addAll(reducedStatements);
						state.logInconsistency();
						return;
					} catch (SQLException e) {
						state.logInconsistency(e);
					}
				}

				private int getNrRows(Connection con) throws SQLException {
					int nrRows;
					try (Statement s = con.createStatement()) {
						ResultSet query = s.executeQuery("SELECT COUNT(*) FROM test");
						query.next();
						nrRows = query.getInt(1);
						return nrRows;
					}
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
