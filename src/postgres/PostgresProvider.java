package postgres;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.DatabaseProvider;
import lama.IgnoreMeException;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.StateToReproduce.PostgresStateToReproduce;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3Common;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresTable;
import postgres.ast.PostgresExpression;
import postgres.gen.PostgresAlterTableGenerator;
import postgres.gen.PostgresAnalyzeGenerator;
import postgres.gen.PostgresClusterGenerator;
import postgres.gen.PostgresDeleteGenerator;
import postgres.gen.PostgresDiscardGenerator;
import postgres.gen.PostgresDropIndex;
import postgres.gen.PostgresIndexGenerator;
import postgres.gen.PostgresInsertGenerator;
import postgres.gen.PostgresStatisticsGenerator;
import postgres.gen.PostgresQueryGenerator;
import postgres.gen.PostgresReindexGenerator;
import postgres.gen.PostgresSetGenerator;
import postgres.gen.PostgresTableGenerator;
import postgres.gen.PostgresTruncateGenerator;
import postgres.gen.PostgresUpdateGenerator;
import postgres.gen.PostgresVacuumGenerator;

// EXISTS
// IN
public class PostgresProvider implements DatabaseProvider {
	
	public static final boolean IS_POSTGRES_TWELVE = true;

	private static final int NR_QUERIES_PER_TABLE = 100000;
	Randomly r = new Randomly();
	private QueryManager manager;

	private enum Action {
		ANALYZE, ALTER_TABLE, CLUSTER, COMMIT, CREATE_STATISTICS, DROP_STATISTICS, DELETE, DISCARD, DROP_INDEX, INSERT, UPDATE, TRUNCATE, VACUUM, REINDEX,
		SET, CREATE_INDEX;
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}
		this.manager = manager;
		PostgresSchema newSchema = PostgresSchema.fromConnection(con, databaseName);
		while (newSchema.getDatabaseTables().size() < Randomly.smallNumber() + 1) {
			try {
				String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
				Query createTable = PostgresTableGenerator.generate(tableName, r, newSchema);
				if (options.logEachSelect()) {
					logger.writeCurrent(createTable.getQueryString());
				}
				manager.execute(createTable);
				newSchema = PostgresSchema.fromConnection(con, databaseName);
			} catch (IgnoreMeException e) {
				
			}
		}

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
//			case DISCARD:
//			case CREATE_INDEX:
//			case CLUSTER:
//				nrPerformed = 0;
//					break;
//			case CREATE_STATISTICS:
//			case DROP_STATISTICS:
//
//				nrPerformed = r.getInteger(0, 30000);
//				break;
//			case COMMIT:
//			case ANALYZE:
//
//				nrPerformed = r.getInteger(0, 300);
//				break;
//			case TRUNCATE:
//			case DROP_INDEX:
//			case ALTER_TABLE:
////				nrPerformed = r.getInteger(0, 10);
//				nrPerformed = 0;
//				break;
//			case REINDEX:
//				nrPerformed = 0;
//				break;
//			case DELETE:
//			case VACUUM:
//				nrPerformed = 0;//r.getInteger(0, 2);
//				break;
//			case UPDATE:
//			case SET:
//				nrPerformed = 500;
//				break;
//			case INSERT:
//				nrPerformed = 300;
//				break;
//			default:
//				throw new AssertionError(action);
//			}
			case DISCARD:
			case CREATE_INDEX:
			case CLUSTER:
				nrPerformed = r.getInteger(0, 100);
					break;
			case CREATE_STATISTICS:
			case DROP_STATISTICS:
				nrPerformed = r.getInteger(0, 30);
				break;
			case COMMIT:
				nrPerformed = r.getInteger(0, 30);
				break;
			case TRUNCATE:
			case DROP_INDEX:
			case ALTER_TABLE:
			case REINDEX:
				nrPerformed = r.getInteger(0, 10);
				break;
			case DELETE:
			case ANALYZE:
			case VACUUM:
				nrPerformed = r.getInteger(0, 5);
				break;
			case UPDATE:
			case SET:
				nrPerformed = 500;
				break;
			case INSERT:
				nrPerformed = 300;
				break;
			default:
				throw new AssertionError(action);
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
			try {
				switch (nextAction) {
				case ANALYZE:
					query = PostgresAnalyzeGenerator.create(newSchema.getRandomTable());
					break;
				case CLUSTER:
					query = PostgresClusterGenerator.create(newSchema);
				case ALTER_TABLE:
					query = PostgresAlterTableGenerator.create(newSchema.getRandomTable(), r, newSchema);
					break;
				case COMMIT:
					if (Randomly.getBoolean()) {
						query = new QueryAdapter("COMMIT") {
							@Override
							public boolean couldAffectSchema() {
								return true;
							}
						};
					} else if (Randomly.getBoolean()) {
						query = ExecuteTransaction.executeBegin();
					
					} else {
						query = new QueryAdapter("ROLLBACK") {
							@Override
							public boolean couldAffectSchema() {
								return true;
							}
						};
					}
					break;
				case CREATE_INDEX:
					query = PostgresIndexGenerator.generate(newSchema, r);
					break;
				case DISCARD:
					query = PostgresDiscardGenerator.create(newSchema);
					break;
				case DELETE:
					query = PostgresDeleteGenerator.create(newSchema.getRandomTable(), r);
					break;
				case DROP_INDEX:
					query = PostgresDropIndex.create(newSchema.getRandomTable().getIndexes());
					break;
				case UPDATE:
					query = PostgresUpdateGenerator.create(newSchema.getRandomTable(), r);
					break;
				case VACUUM:
					query = PostgresVacuumGenerator.create(newSchema.getRandomTable());
					break;
				case REINDEX:
					query = PostgresReindexGenerator.create(newSchema);
					break;
				case TRUNCATE:
					query = PostgresTruncateGenerator.create(newSchema);
					break;
				case SET:
					query = PostgresSetGenerator.create(r);
					break;
				case INSERT:
					query = PostgresInsertGenerator.insert(newSchema.getRandomTable(), r);
					break;
				case CREATE_STATISTICS:
					query = PostgresStatisticsGenerator.insert(newSchema, r);
					break;
				case DROP_STATISTICS:
					query = PostgresStatisticsGenerator.remove(newSchema);
					break;
				default:
					throw new AssertionError(nextAction);
				}
			} catch (IgnoreMeException e) {
				total--;
				continue;
			}
			try {
				if (options.logEachSelect()) {
					logger.writeCurrent(query.getQueryString());
				}
				manager.execute(query);
				if (query.couldAffectSchema()) {
					newSchema = PostgresSchema.fromConnection(con, databaseName);
				}
			} catch (Throwable t) {
				if (t.getMessage().contains("current transaction is aborted")) {
					manager.execute(new QueryAdapter("ABORT"));
					newSchema = PostgresSchema.fromConnection(con, databaseName);
				} else {
					System.err.println(query.getQueryString());
					throw t;
				}
			}
			total--;
		}
		manager.execute(new QueryAdapter("COMMIT"));
		newSchema = PostgresSchema.fromConnection(con, databaseName);

		for (PostgresTable t : newSchema.getDatabaseTables()) {
			if (!ensureTableHasRows(con, t, r)) {
				return;
			}
		}

		newSchema = PostgresSchema.fromConnection(con, databaseName);
		manager.execute(new QueryAdapter("SET LOCAL statement_timeout = 10000;\n"));

		PostgresQueryGenerator queryGenerator = new PostgresQueryGenerator(manager, r, con, databaseName);
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
			try {
				queryGenerator.generateAndCheckQuery((PostgresStateToReproduce) state, logger, options);
			} catch (IgnoreMeException e) {

			}
			manager.incrementSelectQueryCount();
		}

	}

	private boolean ensureTableHasRows(Connection con, PostgresTable randomTable, Randomly r) throws SQLException {
		int nrRows;
		int counter = 5;
		do {
			try {
				Query q = PostgresInsertGenerator.insert(randomTable, r);
				manager.execute(q);
			} catch (SQLException e) {
				if (!QueryGenerator.shouldIgnoreException(e)) {
					throw new AssertionError(e);
				}
			}
			nrRows = getNrRows(con, randomTable);
		} while (nrRows == 0 && counter-- != 0);
		return nrRows != 0;
	}

	public static int getNrRows(Connection con, PostgresTable table) throws SQLException {
		try (Statement s = con.createStatement()) {
			try (ResultSet query = s.executeQuery("SELECT COUNT(*) FROM " + table.getName())) {
				query.next();
				return query.getInt(1);
			}
		}
	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		state.statements.add(new QueryAdapter("\\c test;"));
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
		state.statements.add(new QueryAdapter("CREATE DATABASE " + databaseName));
		state.statements.add(new QueryAdapter("\\c " + databaseName));
		String url = "jdbc:postgresql://localhost:5432/test";
		Connection con = DriverManager.getConnection(url, "lama", "password");
		try (Statement s = con.createStatement()) {
			s.execute("DROP DATABASE IF EXISTS " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute("CREATE DATABASE " + databaseName);
		}
		con.close();
		con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName, "lama", "password");

		return con;
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "postgres";
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		PostgresStateToReproduce specificState = (PostgresStateToReproduce) state;
		if (specificState.getRandomRowValues() != null) {
			List<PostgresColumn> columnList = specificState.getRandomRowValues().keySet().stream()
					.collect(Collectors.toList());
			List<PostgresTable> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
					.collect(Collectors.toList());
			for (PostgresTable t : tableList) {
				sb.append("-- " + t.getName() + "\n");
				List<PostgresColumn> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
						.collect(Collectors.toList());
				for (PostgresColumn c : columnsForTable) {
					sb.append("--\t");
					sb.append(c);
					sb.append("=");
					sb.append(specificState.getRandomRowValues().get(c));
					sb.append("\n");
				}
			}
			sb.append("expected values: \n");
			PostgresExpression whereClause = ((PostgresStateToReproduce) state).getWhereClause();
			if (whereClause != null) {
				sb.append(PostgresVisitor.asExpectedValues(whereClause).replace("\n", "\n-- "));
			}
		}
		try {
			writer.write(sb.toString());
			writer.flush();
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new PostgresStateToReproduce(databaseName);
	}

	@Override
	public Query checkIfRowIsStillContained(StateToReproduce state) {
		return new QueryAdapter(((PostgresStateToReproduce) state).queryThatSelectsRow);
	}

}
