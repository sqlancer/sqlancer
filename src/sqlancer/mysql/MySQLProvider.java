package sqlancer.mysql;

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

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.MySQLStateToReproduce;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLAlterTable;
import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.gen.MySQLDropIndex;
import sqlancer.mysql.gen.MySQLRowInserter;
import sqlancer.mysql.gen.MySQLSetGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.mysql.gen.admin.MySQLFlush;
import sqlancer.mysql.gen.admin.MySQLReset;
import sqlancer.mysql.gen.datadef.CreateIndexGenerator;
import sqlancer.mysql.gen.tblmaintenance.MySQLAnalyzeTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLCheckTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLChecksum;
import sqlancer.mysql.gen.tblmaintenance.MySQLOptimize;
import sqlancer.mysql.gen.tblmaintenance.MySQLRepair;
import sqlancer.sqlite3.gen.SQLite3Common;

public class MySQLProvider implements DatabaseProvider<MySQLGlobalState> {

	private final Randomly r = new Randomly();
	private QueryManager manager;
	private String databaseName;

	enum Action {
		SHOW_TABLES, INSERT, SET_VARIABLE, REPAIR, OPTIMIZE, CHECKSUM, CHECK_TABLE, ANALYZE_TABLE, FLUSH, RESET,
		CREATE_INDEX, ALTER_TABLE, TRUNCATE_TABLE, SELECT_INFO, CREATE_TABLE, DELETE, DROP_INDEX;
	}

	private static int mapActions(MySQLGlobalState globalState, Action a) {
		Randomly r = globalState.getRandomly();
		int nrPerformed = 0;
		switch (a) {
		case DROP_INDEX:
			nrPerformed = r.getInteger(0, 2);
			break;
		case SHOW_TABLES:
			nrPerformed = r.getInteger(0, 1);
			break;
		case CREATE_TABLE:
			nrPerformed = r.getInteger(0, 1);
			break;
		case INSERT:
			nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
			break;
		case REPAIR:
			nrPerformed = r.getInteger(0, 1);
			break;
		case SET_VARIABLE:
			nrPerformed = r.getInteger(0, 5);
			break;
		case CREATE_INDEX:
			nrPerformed = r.getInteger(0, 5);
			break;
		case FLUSH:
			nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
			break;
		case OPTIMIZE:
			// seems to yield low CPU utilization
			nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
			break;
		case RESET:
			// affects the global state, so do not execute
			nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
			break;
		case CHECKSUM:
		case CHECK_TABLE:
		case ANALYZE_TABLE:
			nrPerformed = r.getInteger(0, 2);
		case ALTER_TABLE:
			nrPerformed = r.getInteger(0, 5);
			break;
		case TRUNCATE_TABLE:
			nrPerformed = r.getInteger(0, 2);
			break;
		case SELECT_INFO:
			nrPerformed = r.getInteger(0, 10);
		case DELETE:
			nrPerformed = r.getInteger(0, 10);
			break;
		default:
			throw new AssertionError(a);
		}
		return nrPerformed;
	}

	@Override
	public void generateAndTestDatabase(MySQLGlobalState globalState) throws SQLException {
		this.databaseName = globalState.getDatabaseName();
		this.manager = globalState.getManager();
		Connection con = globalState.getConnection();
		MainOptions options = globalState.getOptions();
		StateLogger logger = globalState.getLogger();
		StateToReproduce state = globalState.getState();
		globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}

		while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
			String tableName = SQLite3Common.createTableName(globalState.getSchema().getDatabaseTables().size());
			Query createTable = MySQLTableGenerator.generate(tableName, r, globalState.getSchema());
			if (options.logEachSelect()) {
				logger.writeCurrent(createTable.getQueryString());
			}
			manager.execute(createTable);
			globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));
		}

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = mapActions(globalState, action);

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
				case SHOW_TABLES:
					query = new QueryAdapter("SHOW TABLES");
					break;
				case INSERT:
					query = MySQLRowInserter.insertRow(globalState);
					break;
				case SET_VARIABLE:
					query = MySQLSetGenerator.set(globalState);
					break;
				case REPAIR:
					query = MySQLRepair.repair(globalState);
					break;
				case OPTIMIZE:
					query = MySQLOptimize.optimize(globalState);
					break;
				case CHECKSUM:
					query = MySQLChecksum.checksum(globalState);
					break;
				case CHECK_TABLE:
					query = MySQLCheckTable.check(globalState);
					break;
				case ANALYZE_TABLE:
					query = MySQLAnalyzeTable.analyze(globalState);
					break;
				case FLUSH:
					query = MySQLFlush.create(globalState);
					break;
				case RESET:
					query = MySQLReset.create(globalState);
					break;
				case CREATE_INDEX:
					query = CreateIndexGenerator.create(globalState);
					break;
				case ALTER_TABLE:
					query = MySQLAlterTable.create(globalState);
					break;
				case SELECT_INFO:
					query = new QueryAdapter(
							"select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
									+ databaseName + "'");
					break;
				case TRUNCATE_TABLE:
					query = new QueryAdapter("TRUNCATE TABLE " + globalState.getSchema().getRandomTable().getName()) {
						@Override
						public boolean execute(Connection con) throws SQLException {
							try {
								super.execute(con);
								return true;
							} catch (SQLException e) {
								if (e.getMessage().contains("doesn't have this option")) {
									return false;
								} else {
									throw e;
								}
							}
						}

					};
					break;
				case CREATE_TABLE:
					String tableName = SQLite3Common.createTableName(globalState.getSchema().getDatabaseTables().size());
					query = MySQLTableGenerator.generate(tableName, r, globalState.getSchema());
					break;
				case DELETE:
					query = MySQLDeleteGenerator.delete(globalState);
					break;
				case DROP_INDEX:
					query = MySQLDropIndex.generate(globalState);
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
					globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));
				}
			} catch (Throwable t) {
				System.err.println(query.getQueryString());
				throw t;
			}
			total--;
		}
//		for (MySQLTable t : globalState.getSchema().getDatabaseTables()) {
//			if (!ensureTableHasRows(con, t, r)) {
//				return;
//			}
//		}

		globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));

		MySQLQueryGenerator queryGenerator = new MySQLQueryGenerator(manager, r, con, databaseName);
		for (int i = 0; i < options.getNrQueries(); i++) {
			try {
				queryGenerator.generateAndCheckQuery((MySQLStateToReproduce) state, logger, options);
			} catch (IgnoreMeException e) {

			}
			manager.incrementSelectQueryCount();
		}

	}

//	private boolean ensureTableHasRows(Connection con, MySQLTable randomTable, Randomly r) throws SQLException {
//		int nrRows;
//		int counter = 1;
//		do {
//			try {
//				Query q = MySQLRowInserter.insertRow(randomTable, r);
//				manager.execute(q);
//			} catch (SQLException e) {
//				if (!SQLite3PivotedQuerySynthesizer.shouldIgnoreException(e)) {
//					throw new AssertionError(e);
//				}
//			}
//			nrRows = getNrRows(con, randomTable);
//		} while (nrRows == 0 && counter-- != 0);
//		return nrRows != 0;
//	}

	public static int getNrRows(Connection con, MySQLTable table) throws SQLException {
		try (Statement s = con.createStatement()) {
			try (ResultSet query = s.executeQuery("SELECT COUNT(*) FROM " + table.getName())) {
				query.next();
				return query.getInt(1);
			}
		}
	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
		state.statements.add(new QueryAdapter("CREATE DATABASE " + databaseName));
		state.statements.add(new QueryAdapter("USE " + databaseName));
		String url = "jdbc:mysql://localhost:3306/?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
		Connection con = DriverManager.getConnection(url, "lama", "password");
		try (Statement s = con.createStatement()) {
			s.execute("DROP DATABASE IF EXISTS " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute("CREATE DATABASE " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute("USE " + databaseName);
		}
		return con;
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "mysql";
	}

	@Override
	public String toString() {
		return String.format("MySQLProvider [database: %s]", databaseName);
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		MySQLStateToReproduce specificState = (MySQLStateToReproduce) state;
		if (specificState.getRandomRowValues() != null) {
			List<MySQLColumn> columnList = specificState.getRandomRowValues().keySet().stream()
					.collect(Collectors.toList());
			List<MySQLTable> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
					.collect(Collectors.toList());
			for (MySQLTable t : tableList) {
				sb.append("-- " + t.getName() + "\n");
				List<MySQLColumn> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
						.collect(Collectors.toList());
				for (MySQLColumn c : columnsForTable) {
					sb.append("--\t");
					sb.append(c);
					sb.append("=");
					sb.append(specificState.getRandomRowValues().get(c));
					sb.append("\n");
				}
			}
			sb.append("expected values: \n");
			sb.append(MySQLVisitor.asExpectedValues(((MySQLStateToReproduce) state).getWhereClause()));
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
		return new MySQLStateToReproduce(databaseName);
	}

	@Override
	public MySQLGlobalState generateGlobalState() {
		return new MySQLGlobalState();
	}

}
