package lama.mariadb;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import lama.DatabaseProvider;
import lama.IgnoreMeException;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.StateToReproduce.MariaDBStateToReproduce;
import lama.mariadb.gen.MariaDBIndexGenerator;
import lama.mariadb.gen.MariaDBInsertGenerator;
import lama.mariadb.gen.MariaDBSetGenerator;
import lama.mariadb.gen.MariaDBTableAdminCommandGenerator;
import lama.mariadb.gen.MariaDBTableGenerator;
import lama.mariadb.gen.MariaDBTruncateGenerator;
import lama.mariadb.gen.MariaDBUpdateGenerator;
import lama.sqlite3.gen.SQLite3Common;

public class MariaDBProvider implements DatabaseProvider {

	public static final int MAX_EXPRESSION_DEPTH = 3;
	private final Randomly r = new Randomly();
	private String databaseName;

	enum Action {
		ANALYZE_TABLE, //
		CHECKSUM, //
		CHECK_TABLE, //
		CREATE_INDEX, //
		INSERT, //
		OPTIMIZE, //
		REPAIR_TABLE, //
		SET, //
		TRUNCATE, //
		UPDATE, //
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		this.databaseName = databaseName;
		MariaDBSchema newSchema = MariaDBSchema.fromConnection(con, databaseName);
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}

		while (newSchema.getDatabaseTables().size() < Randomly.smallNumber() + 1) {
			String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
			Query createTable = MariaDBTableGenerator.generate(tableName, r, newSchema);
			if (options.logEachSelect()) {
				logger.writeCurrent(createTable.getQueryString());
			}
			manager.execute(createTable);
			newSchema = MariaDBSchema.fromConnection(con, databaseName);
		}

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case CHECKSUM:
			case CHECK_TABLE:
			case TRUNCATE:
			case REPAIR_TABLE:
			case OPTIMIZE:
			case ANALYZE_TABLE:
			case UPDATE:
			case CREATE_INDEX:
				nrPerformed = r.getInteger(0, 2);
				break;
			case SET:
				nrPerformed = 20;
				break;
			case INSERT:
				nrPerformed = r.getInteger(0, options.getMaxNumberInserts());
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
				case CHECKSUM:
					query = MariaDBTableAdminCommandGenerator.checksumTable(newSchema);
					break;
				case CHECK_TABLE:
					query = MariaDBTableAdminCommandGenerator.checkTable(newSchema);
					break;
				case TRUNCATE:
					query = MariaDBTruncateGenerator.truncate(newSchema);
					break;
				case REPAIR_TABLE:
					query = MariaDBTableAdminCommandGenerator.repairTable(newSchema);
					break;
				case INSERT:
					query = MariaDBInsertGenerator.insert(newSchema, r);
					break;
				case OPTIMIZE:
					query = MariaDBTableAdminCommandGenerator.optimizeTable(newSchema);
					break;
				case ANALYZE_TABLE:
					query = MariaDBTableAdminCommandGenerator.analyzeTable(newSchema);
					break;
				case UPDATE:
					query = MariaDBUpdateGenerator.update(newSchema, r);
					break;
				case CREATE_INDEX:
					query = MariaDBIndexGenerator.generate(newSchema);
					break;
				case SET:
					query = MariaDBSetGenerator.set(r, options);
					break;
//				case SET_VARIABLE:
//					query = MariaDBSetGenerator.set(r, options);
//					break;
//				case REPAIR:
//					query = MariaDBRepair.repair(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
//					break;
//				case OPTIMIZE:
//					query = MariaDBOptimize.optimize(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
//					break;
//				case CHECKSUM:
//					query = MariaDBChecksum.checksum(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
//					break;
//				case CHECK_TABLE:
//					query = MariaDBCheckTable.check(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
//					break;
//				case ANALYZE_TABLE:
//					query = MariaDBAnalyzeTable.analyze(newSchema.getDatabaseTablesRandomSubsetNotEmpty(), r);
//					break;
//				case FLUSH:
//					query = MariaDBFlush.create(newSchema.getDatabaseTablesRandomSubsetNotEmpty());
//					break;
//				case RESET:
//					query = MariaDBReset.create();
//					break;
//				case CREATE_INDEX:
//					query = createIndexGenerator.create();
//					break;
//				case ALTER_TABLE:
//					query = MariaDBAlterTable.create(newSchema, r);
//					break;
//				case SELECT_INFO:
//					query = new QueryAdapter(
//							"select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
//									+ databaseName + "'");
//					break;
//				case TRUNCATE_TABLE:
//					query = new QueryAdapter("TRUNCATE TABLE " + newSchema.getRandomTable().getName()) {
//						@Override
//						public void execute(Connection con) throws SQLException {
//							try {
//								super.execute(con);
//							} catch (SQLException e) {
//								if (e.getMessage().contains("doesn't have this option")) {
//									return;
//								} else {
//									throw e;
//								}
//							}
//						}
//
//					};
//					break;
//				case CREATE_TABLE:
//					String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
//					query = MariaDBTableGenerator.generate(tableName, r, newSchema);
//					break;
//				case DELETE:
//					query = MariaDBDeleteGenerator.delete(newSchema.getRandomTable(), r);
//					break;
//				case DROP_INDEX:
//					query = MariaDBDropIndex.generate(newSchema.getRandomTable());
//					break;
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
					newSchema = MariaDBSchema.fromConnection(con, databaseName);
//					createIndexGenerator.setNewSchema(newSchema);
				}
			} catch (Throwable t) {
				System.err.println(query.getQueryString());
				throw t;
			}
			total--;
		}
//		for (MariaDBTable t : newSchema.getDatabaseTables()) {
//			if (!ensureTableHasRows(con, t, r)) {
//				return;
//			}
//		}
//
		newSchema = MariaDBSchema.fromConnection(con, databaseName);
//
		MariaDBMetamorphicQuerySynthesizer queryGenerator = new MariaDBMetamorphicQuerySynthesizer(newSchema, r, con, (MariaDBStateToReproduce) state);
		for (int i = 0; i < options.getNrQueries(); i++) {
			try {
				queryGenerator.generateAndCheck();
			} catch (IgnoreMeException e) {

			}
			manager.incrementSelectQueryCount();
		}

	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
		state.statements.add(new QueryAdapter("CREATE DATABASE " + databaseName));
		state.statements.add(new QueryAdapter("USE " + databaseName));
		// /?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true
		String url = "jdbc:mariadb://localhost:3306";
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
		return "mariadb";
	}

	@Override
	public String toString() {
		return String.format("MariaDBProvider [database: %s]", databaseName);
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new MariaDBStateToReproduce(databaseName);
	}

}
