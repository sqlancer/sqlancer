package lama.postgres;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
import lama.postgres.PostgresSchema.PostgresColumn;
import lama.postgres.PostgresSchema.PostgresTable;
import lama.postgres.ast.PostgresExpression;
import lama.postgres.gen.PostgresAlterTableGenerator;
import lama.postgres.gen.PostgresAnalyzeGenerator;
import lama.postgres.gen.PostgresClusterGenerator;
import lama.postgres.gen.PostgresCommentGenerator;
import lama.postgres.gen.PostgresDeleteGenerator;
import lama.postgres.gen.PostgresDiscardGenerator;
import lama.postgres.gen.PostgresDropIndex;
import lama.postgres.gen.PostgresIndexGenerator;
import lama.postgres.gen.PostgresInsertGenerator;
import lama.postgres.gen.PostgresNotifyGenerator;
import lama.postgres.gen.PostgresQueryCatalogGenerator;
import lama.postgres.gen.PostgresReindexGenerator;
import lama.postgres.gen.PostgresSequenceGenerator;
import lama.postgres.gen.PostgresSetGenerator;
import lama.postgres.gen.PostgresStatisticsGenerator;
import lama.postgres.gen.PostgresTableGenerator;
import lama.postgres.gen.PostgresTransactionGenerator;
import lama.postgres.gen.PostgresTruncateGenerator;
import lama.postgres.gen.PostgresUpdateGenerator;
import lama.postgres.gen.PostgresVacuumGenerator;
import lama.postgres.gen.PostgresViewGenerator;
import lama.sqlite3.gen.SQLite3Common;

// EXISTS
// IN
public class PostgresProvider implements DatabaseProvider {

	public static final boolean IS_POSTGRES_TWELVE = true;

	public static boolean GENERATE_ONLY_KNOWN = false;


	private List<String> collationNames;

	private enum Action {
		ANALYZE, ALTER_TABLE, CLUSTER, COMMIT, CREATE_STATISTICS, DROP_STATISTICS, DELETE, DISCARD, DROP_INDEX, INSERT,
		UPDATE, TRUNCATE, VACUUM, REINDEX, SET, CREATE_INDEX, SET_CONSTRAINTS, RESET_ROLE, COMMENT_ON, RESET, NOTIFY,
		LISTEN, UNLISTEN, CREATE_SEQUENCE, CREATE_VIEW, QUERY_CATALOG;
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		Randomly r = new Randomly();
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}
		List<String> opClasses = getOpclasses(con);
		List<String> operators = getOperators(con);
		PostgresGlobalState globalState = new PostgresGlobalState(opClasses, operators, collationNames);
		PostgresSchema newSchema = PostgresSchema.fromConnection(con, databaseName);
		while (newSchema.getDatabaseTables().size() < 2) {
			try {
				String tableName = SQLite3Common.createTableName(newSchema.getDatabaseTables().size());
				Query createTable = PostgresTableGenerator.generate(tableName, r, newSchema, GENERATE_ONLY_KNOWN,
						globalState);
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
			case CREATE_INDEX:
				nrPerformed = r.getInteger(0, 10);
				break;
			case CREATE_STATISTICS:
				nrPerformed = r.getInteger(0, 5);
				break;
			case DISCARD:
			case CLUSTER:
			case DROP_INDEX:
				nrPerformed = r.getInteger(0, 5);
				break;
			case COMMIT:
				nrPerformed = r.getInteger(0, 0);
				break;
			case ALTER_TABLE:
				nrPerformed = r.getInteger(0, 5);
				break;
			case REINDEX:
			case RESET:
				nrPerformed = r.getInteger(0, 5);
				break;
			case DELETE:
			case RESET_ROLE:
			case SET:
			case QUERY_CATALOG:
				nrPerformed = r.getInteger(0, 5);
				break;
			case ANALYZE:
				nrPerformed = r.getInteger(0, 10);
				break;
			case VACUUM:
			case SET_CONSTRAINTS:
			case COMMENT_ON:
			case NOTIFY:
			case LISTEN:
			case UNLISTEN:
			case CREATE_SEQUENCE:
			case DROP_STATISTICS:
			case TRUNCATE:
				nrPerformed = r.getInteger(0, 2);
				break;
			case CREATE_VIEW:
				nrPerformed = r.getInteger(0, 2);
				break;
			case UPDATE:
				nrPerformed = r.getInteger(0, 10);
				break;
			case INSERT:
				nrPerformed = r.getInteger(0, 30);
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
			boolean successful = false;
			int tries = 0;
			while (!successful && tries++ <= 100) {
				try {
					switch (nextAction) {
					case ANALYZE:
						query = PostgresAnalyzeGenerator.create(newSchema.getRandomTable());
						break;
					case CLUSTER:
						query = PostgresClusterGenerator.create(newSchema);
					case ALTER_TABLE:
						query = PostgresAlterTableGenerator.create(newSchema.getRandomTable(t -> !t.isView()), r,
								newSchema, GENERATE_ONLY_KNOWN, opClasses, operators);
						break;
					case SET_CONSTRAINTS:
						StringBuilder sb = new StringBuilder();
						sb.append("SET CONSTRAINTS ALL ");
						sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE"));
						query = new QueryAdapter(sb.toString());
						break;
					case RESET_ROLE:
						query = new QueryAdapter("RESET ROLE");
						break;
					case QUERY_CATALOG:
						query = PostgresQueryCatalogGenerator.query();
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
							query = PostgresTransactionGenerator.executeBegin();

						} else {
							query = new QueryAdapter("ROLLBACK") {
								@Override
								public boolean couldAffectSchema() {
									return true;
								}
							};
						}
						break;
					case NOTIFY:
						query = PostgresNotifyGenerator.createNotify(r);
						break;
					case LISTEN:
						query = PostgresNotifyGenerator.createListen();
						break;
					case UNLISTEN:
						query = PostgresNotifyGenerator.createUnlisten();
						break;
					case RESET:
						// https://www.postgresql.org/docs/devel/sql-reset.html
						// TODO: also configuration parameter
						query = new QueryAdapter("RESET ALL");
						break;
					case COMMENT_ON:
						query = PostgresCommentGenerator.generate(newSchema, r);
						break;
					case CREATE_INDEX:
						query = PostgresIndexGenerator.generate(newSchema, r, globalState);
						break;
					case DISCARD:
						query = PostgresDiscardGenerator.create(newSchema);
						break;
					case CREATE_VIEW:
						query = PostgresViewGenerator.create(newSchema, r);
						break;
					case DELETE:
						query = PostgresDeleteGenerator.create(newSchema.getRandomTable(t -> !t.isView()), r);
						break;
					case DROP_INDEX:
						query = PostgresDropIndex.create(newSchema.getRandomTable().getIndexes());
						break;
					case UPDATE:
						query = PostgresUpdateGenerator.create(newSchema.getRandomTable(t -> t.isInsertable()), r);
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
						query = PostgresInsertGenerator.insert(newSchema.getRandomTable(t -> t.isInsertable()), r);
						break;
					case CREATE_STATISTICS:
						query = PostgresStatisticsGenerator.insert(newSchema, r);
						break;
					case DROP_STATISTICS:
						query = PostgresStatisticsGenerator.remove(newSchema);
						break;
					case CREATE_SEQUENCE:
						query = PostgresSequenceGenerator.createSequence(r, newSchema);
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
					successful = manager.execute(query);
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
			}
			total--;
		}
		manager.execute(new QueryAdapter("COMMIT"));
		newSchema = PostgresSchema.fromConnection(con, databaseName);

		newSchema = PostgresSchema.fromConnection(con, databaseName);
		manager.execute(new QueryAdapter("SET SESSION statement_timeout = 5000;\n"));

		PostgresMetamorphicOracleGenerator or = new PostgresMetamorphicOracleGenerator(newSchema, r, con,
				(PostgresStateToReproduce) state, logger, options, manager, globalState);
		for (int i = 0; i < options.getNrQueries(); i++) {
			try {
				or.generateAndCheck();
			} catch (IgnoreMeException e) {
				continue;
			}
			manager.incrementSelectQueryCount();
		}

	}

	private List<String> getCollnames(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s
					.executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
	}

	private List<String> getOpclasses(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
	}

	private List<String> getOperators(Connection con) throws SQLException {
		List<String> opClasses = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
				while (rs.next()) {
					opClasses.add(rs.getString(1));
				}
			}
		}
		return opClasses;
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
		String url = "jdbc:postgresql://localhost:5432/test";
		Connection con = DriverManager.getConnection(url, "lama", "password");
		collationNames = getCollnames(con);
		state.statements.add(new QueryAdapter("\\c test;"));
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
		String createDatabaseCommand = getCreateDatabaseCommand(databaseName);
		state.statements.add(new QueryAdapter(createDatabaseCommand));
		state.statements.add(new QueryAdapter("\\c " + databaseName));
		try (Statement s = con.createStatement()) {
			s.execute("DROP DATABASE IF EXISTS " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute(createDatabaseCommand);
		}
		con.close();
		con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName, "lama", "password");
		List<String> statements = Arrays.asList("CREATE EXTENSION IF NOT EXISTS btree_gin;",
				"CREATE EXTENSION IF NOT EXISTS btree_gist;", "CREATE EXTENSION IF NOT EXISTS pg_prewarm;",
				"SET max_parallel_workers_per_gather=16");
		for (String s : statements) {
			QueryAdapter query = new QueryAdapter(s);
			state.statements.add(query);
			query.execute(con);
		}
//		new QueryAdapter("set jit_above_cost = 0; set jit_inline_above_cost = 0; set jit_optimize_above_cost = 0;").execute(con);
		return con;
	}

	private String getCreateDatabaseCommand(String databaseName) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE DATABASE " + databaseName + " ");
		if (Randomly.getBoolean()) {
			if (Randomly.getBoolean()) {
				sb.append("WITH ENCODING '");
				sb.append(Randomly.fromOptions("utf8"));
				sb.append("' ");
			}
			for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
				if (Randomly.getBoolean()) {
					sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(collationNames)));
				}
			}
			sb.append(" TEMPLATE template0");
		}
		return sb.toString();
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

}
