package sqlancer.tidb;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.DatabaseProvider;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.MySQLStateToReproduce;
import sqlancer.mysql.MySQLSchema;

public class TiDBProvider implements DatabaseProvider {

	@FunctionalInterface
	public interface TiDBQueryProvider {

		Query getQuery(TiDBGlobalState globalState) throws SQLException;
	}

	public static enum Action {
		INSERT((g) -> new QueryAdapter("INSERT INTO t0 VALUES (" + Randomly.getNonCachedInteger() + ")")), //
		ANALYZE_TABLE((g) -> new QueryAdapter("ANALYZE TABLE " + g.getSchema().getRandomTable().getName()));

		private final TiDBQueryProvider queryProvider;

		private Action(TiDBQueryProvider queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(TiDBGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	public static class TiDBGlobalState extends GlobalState {

		private MySQLSchema schema;

		public void setSchema(MySQLSchema schema) {
			this.schema = schema;
		}

		public MySQLSchema getSchema() {
			return schema;
		}

	}

	@FunctionalInterface
	public interface AfterQueryAction {
		public void notify(Query q) throws SQLException;
	}

	@FunctionalInterface
	public interface ActionMapper<T> {
		public int map(T globalState, Action a);
	}

	public class TiDBStatementExecutor {

		private final TiDBGlobalState globalState;
		private final Action[] actions;
		private final ActionMapper<TiDBGlobalState> mapping;
		private final AfterQueryAction queryConsumer;

		TiDBStatementExecutor(TiDBGlobalState globalState, String databaseName, Action[] actions,
				ActionMapper<TiDBGlobalState> mapping, AfterQueryAction queryConsumer) {
			this.globalState = globalState;
			this.actions = actions;
			this.mapping = mapping;
			this.queryConsumer = queryConsumer;
		}

		public void executeStatements() throws SQLException {
			Randomly r = globalState.getRandomly();
			int[] nrRemaining = new int[Action.values().length];
			List<Action> availableActions = new ArrayList<>();
			int total = 0;
			for (int i = 0; i < actions.length; i++) {
				Action action = Action.values()[i];
				int nrPerformed = mapping.map(globalState, action);
				if (nrPerformed != 0) {
					availableActions.add(action);
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
				Query query = null;
				try {
					boolean success;
					int nrTries = 0;
					do {
						query = nextAction.getQuery(globalState);
						if (globalState.getOptions().logEachSelect()) {
							globalState.getLogger().writeCurrent(query.getQueryString());
						}
						success = globalState.getManager().execute(query);
					} while (!success && nrTries++ < 1000);
				} catch (IgnoreMeException e) {

				}
				if (query != null && query.couldAffectSchema()) {
					queryConsumer.notify(query);
					if (globalState.getSchema().getDatabaseTables().isEmpty()) {
						throw new IgnoreMeException();
					}
				}
				total--;
			}
		}
	}

	private static int mapActions(TiDBGlobalState globalState, Action a) {
		Randomly r = globalState.getRandomly();
		switch (a) {
		case ANALYZE_TABLE:
			return r.getInteger(0, 5);
		case INSERT:
			return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
		default:
			throw new AssertionError(a);
		}

	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		Randomly r = new Randomly();
		TiDBGlobalState globalState = new TiDBGlobalState();
		globalState.setConnection(con);
		globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));
		globalState.setRandomly(r);
		globalState.setMainOptions(options);
		globalState.setStateLogger(logger);
		globalState.setManager(manager);
		globalState.setState(state);
//		TiDBOptions TiDBOptions = new TiDBOptions();
//		JCommander.newBuilder().addObject(TiDBOptions).build().parse(options.getDbmsOptions().split(" "));
//		globalState.setTiDBOptions(TiDBOptions);
//

		Query qt = new QueryAdapter("CREATE TABLE t0(c0 INT UNIQUE)");
		manager.execute(qt);

		TiDBStatementExecutor se = new TiDBStatementExecutor(globalState, databaseName, Action.values(),
				TiDBProvider::mapActions, (q) -> {
					if (q.couldAffectSchema())
						globalState.setSchema(MySQLSchema.fromConnection(con, databaseName));
				});
		se.executeStatements();
		manager.incrementCreateDatabase();
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
