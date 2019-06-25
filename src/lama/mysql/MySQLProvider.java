package lama.mysql;

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
import lama.StateToReproduce.MySQLStateToReproduce;
import lama.mysql.MySQLSchema.MySQLColumn;
import lama.mysql.MySQLSchema.MySQLTable;
import lama.mysql.gen.MySQLAlterTable;
import lama.mysql.gen.MySQLRowInserter;
import lama.mysql.gen.MySQLSetGenerator;
import lama.mysql.gen.MySQLTableGenerator;
import lama.mysql.gen.admin.MySQLFlush;
import lama.mysql.gen.admin.MySQLReset;
import lama.mysql.gen.datadef.CreateIndexGenerator;
import lama.mysql.gen.tblmaintenance.MySQLAnalyzeTable;
import lama.mysql.gen.tblmaintenance.MySQLCheckTable;
import lama.mysql.gen.tblmaintenance.MySQLChecksum;
import lama.mysql.gen.tblmaintenance.MySQLOptimize;
import lama.mysql.gen.tblmaintenance.MySQLRepair;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3Common;

public class MySQLProvider implements DatabaseProvider {

	private static final int NR_QUERIES_PER_TABLE = 1000;
	private static final int MAX_INSERT_ROW_TRIES = 30;
	private final Randomly r = new Randomly();
	private QueryManager manager;
	private String databaseName;

	enum Action {
		SHOW_TABLES, INSERT, SET_VARIABLE, REPAIR, OPTIMIZE, CHECKSUM, CHECK_TABLE, ANALYZE_TABLE, FLUSH, RESET,
		CREATE_INDEX, ALTER_TABLE, TRUNCATE_TABLE, SELECT_INFO, CREATE_TABLE;
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		this.databaseName = databaseName;
		this.manager = manager;
		MySQLSchema newSchema = MySQLSchema.fromConnection(con, databaseName);
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}

		while (newSchema.getDatabaseTables().size() < Randomly.smallNumber() + 1) {
			String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
			Query createTable = MySQLTableGenerator.generate(tableName, r, newSchema);
			if (options.logEachSelect()) {
				logger.writeCurrent(createTable.getQueryString());
			}
			manager.execute(createTable);
			newSchema = MySQLSchema.fromConnection(con, databaseName);
		}

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case SHOW_TABLES:
				nrPerformed = r.getInteger(0, 1);
				break;
			case CREATE_TABLE:
				nrPerformed = r.getInteger(0, 1);
				break;
			case INSERT:
				nrPerformed = MAX_INSERT_ROW_TRIES;
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
			case CHECKSUM:
			case CHECK_TABLE:
			case ANALYZE_TABLE:
			case RESET:
				// affects the global state, so do not execute
				nrPerformed = 0;
				break;
			case ALTER_TABLE:
				nrPerformed = r.getInteger(0, 5);
				break;
			case TRUNCATE_TABLE:
				nrPerformed = r.getInteger(0, 2);
				break;
			case SELECT_INFO:
				nrPerformed = r.getInteger(0, 10);
				break;
			}
			if (nrPerformed != 0) {
				actions.add(action);
			}
			nrRemaining[action.ordinal()] = nrPerformed;
			total += nrPerformed;
		}
		CreateIndexGenerator createIndexGenerator = new CreateIndexGenerator(newSchema, r);

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
					query = MySQLRowInserter.insertRow(newSchema.getRandomTable(), r);
					break;
				case SET_VARIABLE:
					query = MySQLSetGenerator.set(r);
					break;
				case REPAIR:
					query = MySQLRepair.repair(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
					break;
				case OPTIMIZE:
					query = MySQLOptimize.optimize(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
					break;
				case CHECKSUM:
					query = MySQLChecksum.checksum(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
					break;
				case CHECK_TABLE:
					query = MySQLCheckTable.check(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
					break;
				case ANALYZE_TABLE:
					query = MySQLAnalyzeTable.analyze(newSchema.getDatabaseTablesRandomSubsetNotEmpty(), r);
					break;
				case FLUSH:
					query = MySQLFlush.create(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
					break;
				case RESET:
					query = MySQLReset.create();
					break;
				case CREATE_INDEX:
					query = createIndexGenerator.create();
					break;
				case ALTER_TABLE:
					query = MySQLAlterTable.create(newSchema, r);
					break;
				case SELECT_INFO:
					query = new QueryAdapter(
							"select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
									+ databaseName + "'");
					break;
				case TRUNCATE_TABLE:
					query = new QueryAdapter("TRUNCATE TABLE " + newSchema.getRandomTable().getName()) {
						@Override
						public void execute(Connection con) throws SQLException {
							try {
								super.execute(con);
							} catch (SQLException e) {
								if (e.getMessage().contains("doesn't have this option")) {
									return;
								} else {
									throw e;
								}
							}
						}

					};
					break;
				case CREATE_TABLE:
					String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
					query = MySQLTableGenerator.generate(tableName, r, newSchema);
					break;
				default:
					throw new AssertionError(nextAction);
				}
			} catch (IgnoreMeException e) {
				continue;
			}
			try {
				if (options.logEachSelect()) {
					logger.writeCurrent(query.getQueryString());
				}
				manager.execute(query);
				if (query.couldAffectSchema()) {
					newSchema = MySQLSchema.fromConnection(con, databaseName);
					createIndexGenerator.setNewSchema(newSchema);
				}
			} catch (Throwable t) {
				System.err.println(query.getQueryString());
				throw t;
			}
			total--;
		}
		for (MySQLTable t : newSchema.getDatabaseTables()) {
			if (!ensureTableHasRows(con, t, r)) {
				return;
			}
		}
		QueryAdapter query = new QueryAdapter(
				"select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + databaseName + "'");
		manager.execute(query);

		newSchema = MySQLSchema.fromConnection(con, databaseName);

		MySQLQueryGenerator queryGenerator = new MySQLQueryGenerator(manager, r, con, databaseName);
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
			try {
				queryGenerator.generateAndCheckQuery((MySQLStateToReproduce) state, logger, options);
			} catch (IgnoreMeException e) {

			}
			manager.incrementSelectQueryCount();
		}

	}

	private boolean ensureTableHasRows(Connection con, MySQLTable randomTable, Randomly r) throws SQLException {
		int nrRows;
		int counter = MAX_INSERT_ROW_TRIES;
		do {
			try {
				Query q = MySQLRowInserter.insertRow(randomTable, r);
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
	public Query checkIfRowIsStillContained(StateToReproduce state) {
		return new QueryAdapter(((MySQLStateToReproduce) state).queryThatSelectsRow);
	}

}
