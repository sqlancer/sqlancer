package sqlancer.tidb;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import sqlancer.DatabaseProvider;
import sqlancer.GlobalState;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.MySQLStateToReproduce;

public class TiDBProvider implements DatabaseProvider {

	@FunctionalInterface
	public interface TiDBQueryProvider {

		Query getQuery(TiDBGlobalState globalState) throws SQLException;
	}

	public static enum Action {
//		INSERT(TiDBInsertGenerator::insert), //
//		TRUNCATE(TiDBTruncateGenerator::truncate), //
//		CREATE_STATISTICS(TiDBCreateStatisticsGenerator::create), //
//		SET_SESSION(TiDBSetSessionGenerator::create), //
//		CREATE_INDEX(TiDBIndexGenerator::create), //
//		UPDATE(TiDBUpdateGenerator::gen), //
//		CREATE_VIEW(TiDBViewGenerator::generate), //
//		SET_CLUSTER_SETTING(TiDBSetClusterSettingGenerator::create), DELETE(TiDBDeleteGenerator::delete),
//		COMMENT_ON(RockroachDBCommentOnGenerator::comment), SHOW(TiDBShowGenerator::show), TRANSACTION((g) -> {
//			String s = Randomly.fromOptions("BEGIN", "ROLLBACK", "COMMIT");
//			return new QueryAdapter(s, Arrays.asList("there is no transaction in progress",
//					"there is already a transaction in progress", "current transaction is aborted"));
//		}), EXPLAIN((g) -> {
//			new TiDBRandomQuerySynthesizer();
//			StringBuilder sb = new StringBuilder("EXPLAIN ");
//			Set<String> errors = new HashSet<>();
//			if (Randomly.getBoolean()) {
//				sb.append("(");
//				sb.append(Randomly.nonEmptySubset("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC").stream()
//						.collect(Collectors.joining(", ")));
//				sb.append(") ");
//				errors.add("cannot set EXPLAIN mode more than once");
//				errors.add("unable to vectorize execution plan");
//				errors.add("unsupported type");
//				errors.add("vectorize is set to 'off'");
//			}
//			sb.append(TiDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1));
//			TiDBErrors.addExpressionErrors(errors);
//			return new QueryAdapter(sb.toString(), errors);
//		}),
//		SCRUB((g) -> new QueryAdapter(
//				"EXPERIMENTAL SCRUB table " + g.getSchema().getRandomTable(t -> !t.isView()).getName(),
//				// https://github.com/TiDB/cockroach/issues/46401
//				Arrays.asList("scrub-fk: column \"t.rowid\" does not exist")));
//
//		private final TiDBQueryProvider queryProvider;
//
//		private Action(TiDBQueryProvider queryProvider) {
//			this.queryProvider = queryProvider;
//		}
//
//		public Query getQuery(TiDBGlobalState state) throws SQLException {
//			return queryProvider.getQuery(state);
//		}
	}



	public static class TiDBGlobalState extends GlobalState {

	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
//		Randomly r = new Randomly();
//		TiDBGlobalState globalState = new TiDBGlobalState();
//		globalState.setConnection(con);
//		globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
//		globalState.setRandomly(r);
//		globalState.setMainOptions(options);
//		globalState.setStateLogger(logger);
//		globalState.setState(state);
//		TiDBOptions TiDBOptions = new TiDBOptions();
//		JCommander.newBuilder().addObject(TiDBOptions).build().parse(options.getDbmsOptions().split(" "));
//		globalState.setTiDBOptions(TiDBOptions);
//
//		manager.execute(new QueryAdapter("SET CLUSTER SETTING debug.panic_on_failed_assertions = true;"));
//		manager.execute(new QueryAdapter("SET CLUSTER SETTING diagnostics.reporting.enabled	 = false;"));
//		manager.execute(
//				new QueryAdapter("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports		 = false;"));
//
//		for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
//			boolean success = false;
//			do {
//				try {
//					Query q = TiDBTableGenerator.generate(globalState);
//					success = manager.execute(q);
//					logger.writeCurrent(state);
//					try {
//						logger.getCurrentFileWriter().close();
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					logger.currentFileWriter = null;
//				} catch (IgnoreMeException e) {
//
//				}
//			} while (!success);
//			globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
//		}
//		logger.writeCurrent(state);
//
//		int[] nrRemaining = new int[Action.values().length];
//		List<Action> actions = new ArrayList<>();
//		int total = 0;
//		for (int i = 0; i < Action.values().length; i++) {
//			Action action = Action.values()[i];
//			int nrPerformed = 0;
//			switch (action) {
//			case INSERT:
//				nrPerformed = r.getInteger(0, options.getMaxNumberInserts());
//				break;
//			case UPDATE:
//				nrPerformed = r.getInteger(0, 3);
//				break;
//			case EXPLAIN:
//				nrPerformed = r.getInteger(0, 10);
//				break;
//			case SHOW:
//			case COMMENT_ON:
//			case TRUNCATE:
//			case DELETE:
//			case CREATE_STATISTICS:
//				nrPerformed = r.getInteger(0, 2);
//				break;
//			case CREATE_VIEW:
//				nrPerformed = r.getInteger(0, 2);
//				break;
//			case SET_SESSION:
//			case SET_CLUSTER_SETTING:
//				nrPerformed = r.getInteger(0, 3);
//				break;
//			case CREATE_INDEX:
//				nrPerformed = r.getInteger(0, 10);
//				break;
//			case SCRUB:
//				nrPerformed = 1;
//				break;
//			case TRANSACTION:
//				nrPerformed = 0; // r.getInteger(0, 0);
//				break;
//			default:
//				throw new AssertionError(action);
//			}
//			if (nrPerformed != 0) {
//				actions.add(action);
//			}
//			nrRemaining[action.ordinal()] = nrPerformed;
//			total += nrPerformed;
//		}
//
//		while (total != 0) {
//			Action nextAction = null;
//			int selection = r.getInteger(0, total);
//			int previousRange = 0;
//			for (int i = 0; i < nrRemaining.length; i++) {
//				if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
//					nextAction = Action.values()[i];
//					break;
//				} else {
//					previousRange += nrRemaining[i];
//				}
//			}
//			assert nextAction != null;
//			assert nrRemaining[nextAction.ordinal()] > 0;
//			nrRemaining[nextAction.ordinal()]--;
//			Query query = null;
//			try {
//				boolean success;
//				int nrTries = 0;
//				do {
//					query = nextAction.getQuery(globalState);
//					if (options.logEachSelect()) {
//						logger.writeCurrent(query.getQueryString());
//					}
//					success = manager.execute(query);
//				} while (!success && nrTries++ < 1000);
//			} catch (IgnoreMeException e) {
//
//			}
//			if (query != null && query.couldAffectSchema()) {
//				globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
//				if (globalState.getSchema().getDatabaseTables().isEmpty()) {
//					throw new IgnoreMeException();
//				}
//			}
//			total--;
//		}
//		manager.incrementCreateDatabase();
//		if (Randomly.getBoolean()) {
//			manager.execute(new QueryAdapter("SET vectorize=on;"));
//		}
//		TestOracle oracle = globalState.getTiDBOptions().oracle.create(globalState);
//		for (int i = 0; i < options.getNrQueries(); i++) {
//			try {
//				oracle.check();
//				manager.incrementSelectQueryCount();
//			} catch (IgnoreMeException e) {
//
//			}
//		}
//		try {
//			if (options.logEachSelect()) {
//				logger.getCurrentFileWriter().close();
//				logger.currentFileWriter = null;
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		String url = "jdbc:mysql://127.0.0.1:4000/";
		Connection con = DriverManager.getConnection(url, "root", "");
		state.statements.add(new QueryAdapter("USE test"));
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName + " CASCADE"));
		String createDatabaseCommand = "CREATE DATABASE " + databaseName;
		state.statements.add(new QueryAdapter(createDatabaseCommand));
		state.statements.add(new QueryAdapter("USE " + databaseName));
		try (Statement s = con.createStatement()) {
			s.execute("DROP DATABASE IF EXISTS " + databaseName);
		}
		try (Statement s = con.createStatement()) {
			s.execute(createDatabaseCommand);
		}
		con.close();
		con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:4000/" + databaseName, "root", "");
		return con;
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "TiDB";
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		// TODO Auto-generated method stub

	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new MySQLStateToReproduce(databaseName);
	}
}
