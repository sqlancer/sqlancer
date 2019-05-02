package lama;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.tablegen.sqlite3.SQLite3IndexGenerator;
import lama.tablegen.sqlite3.SQLite3PragmaGenerator;
import lama.tablegen.sqlite3.SQLite3TableGenerator;
import lama.tablegen.sqlite3.Sqlite3RowGenerator;

public class Main {

	private static final int NR_QUERIES_PER_TABLE = 1000;
	private static final int NR_THREADS = 16;
	public static final int NR_INSERT_ROW_TRIES = 100;
	public static final int EXPRESSION_MAX_DEPTH = 2;

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

		public StateToReproduce(String databaseName) {
			this.databaseName = databaseName;

		}

		public Logger getLogger() {
			if (logger == null) {
				try {
					FileHandler fh = new FileHandler("logs" + File.separator + databaseName + ".log");
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
			String stackTrace = getStackTrace(e1);
			getLogger().severe(stackTrace);
			getLogger().severe(toString());
			this.errorKind = ErrorKind.EXCEPTION;
			return toString();
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

		ExecutorService executor = Executors.newFixedThreadPool(NR_THREADS);
		for (int i = 0; i < 100; i++) {
			final String databaseName = "lama" + i;
			executor.execute(new Runnable() {

				@Override
				public void run() {
					Thread.currentThread().setName(databaseName);
					while (true) {
						StateToReproduce state = new StateToReproduce(databaseName);
						try {
							Connection con = DatabaseFacade.createDatabase(databaseName);
							java.sql.DatabaseMetaData meta = con.getMetaData();
							state.databaseVersion = meta.getDatabaseProductVersion();
							Statement createStatement = con.createStatement();
							SQLite3PragmaGenerator.insertPragmas(con, state, false);
							String tableQuery = SQLite3TableGenerator.createTableStatement("test", state);
							createStatement.execute(tableQuery);
							createStatement.close();
							SQLite3IndexGenerator.insertIndexes(con, state);
							int nrRows;
							do {
								Sqlite3RowGenerator.insertRows(Schema.fromConnection(con).getRandomTable(), con, state);
								nrRows = getNrRows(con);
							} while (nrRows == 0);
							SQLite3PragmaGenerator.insertPragmas(con, state, true);
							QueryGenerator queryGenerator = new QueryGenerator(con);
//							JCommander.newBuilder().addObject(queryGenerator).build().parse(args);
							if (Randomly.getBoolean()) {
								try (Statement s = con.createStatement()) {
									state.statements.add("VACUUM;");
									s.execute("VACUUM;");
								}
							}
							if (Randomly.getBoolean()) {
								try (Statement s = con.createStatement()) {
									state.statements.add("REINDEX;");
									s.execute("REINDEX;");
								}
							}
							for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
								queryGenerator.generateAndCheckQuery(state);
							}
							con.close();
						} catch (ReduceMeException reduce) {
							try {
								List<String> reducedStatements = new ArrayList<>(state.statements);
								int i = 0;
								outer: while (i < reducedStatements.size()) {
									List<String> reducedTryStatements = new ArrayList<>(reducedStatements);
									reducedTryStatements.remove(i);
									Connection con = DatabaseFacade.createDatabase(databaseName + "_reduced");
									try (Statement statement = con.createStatement()) {
										for (String s : reducedTryStatements) {
											statement.execute(s);
										}
									} catch (Throwable t) {
										i++;
										continue outer;
									}
									try (Statement s = con.createStatement()) {
										try {
											ResultSet result = s.executeQuery(state.query);
											boolean isContainedIn = !result.isClosed();
											if (!isContainedIn) {
												String checkRowIsInside = "SELECT * FROM test INTERSECT SELECT " + state.values;
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
								state.statements = new ArrayList<>();
								state.statements.add("-- trying to reduce query");
								state.statements.addAll(reducedStatements);
								state.logInconsistency();
							} catch (SQLException e) {
								state.logInconsistency(e);
							}
						} catch (Throwable e1) {
							// TODO Auto-generated catch block
							state.logInconsistency(e1);
							threadsShutdown++;
//							executor.shutdown();
//							System.exit(-1);
							return;
						}
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

}
