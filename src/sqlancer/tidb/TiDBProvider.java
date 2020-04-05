package sqlancer.tidb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.AbstractAction;
import sqlancer.CompositeTestOracle;
import sqlancer.DatabaseProvider;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.MySQLStateToReproduce;
import sqlancer.StatementExecutor;
import sqlancer.TestOracle;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBAnalyzeTableGenerator;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBSetGenerator;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.gen.TiDBViewGenerator;

public class TiDBProvider implements DatabaseProvider<TiDBGlobalState, TiDBOptions> {

	public static enum Action implements AbstractAction<TiDBGlobalState> {
		INSERT(TiDBInsertGenerator::getQuery), //
		ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery),
		TRUNCATE((g) -> new QueryAdapter("TRUNCATE " + g.getSchema().getRandomTable().getName())),
		CREATE_INDEX(TiDBIndexGenerator::getQuery),
		DELETE(TiDBDeleteGenerator::getQuery),
		SET(TiDBSetGenerator::getQuery),
		ADMIN_CHECKSUM_TABLE((g) -> new QueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())),
		VIEW_GENERATOR(TiDBViewGenerator::getQuery);

		private final QueryProvider<TiDBGlobalState> queryProvider;

		private Action(QueryProvider<TiDBGlobalState> queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(TiDBGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	public static class TiDBGlobalState extends GlobalState<TiDBOptions> {

		private TiDBSchema schema;

		public void setSchema(TiDBSchema schema) {
			this.schema = schema;
		}

		public TiDBSchema getSchema() {
			return schema;
		}

	}

	private static int mapActions(TiDBGlobalState globalState, Action a) {
		Randomly r = globalState.getRandomly();
		switch (a) {
		case ANALYZE_TABLE:
		case CREATE_INDEX:
			return r.getInteger(0, 5);
		case INSERT:
			return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
		case TRUNCATE:
		case DELETE:
		case ADMIN_CHECKSUM_TABLE:
			return r.getInteger(0, 2);
		case SET:
			return r.getInteger(0, 5);
		case VIEW_GENERATOR:
			return r.getInteger(0, 0);
		default:
			throw new AssertionError(a);
		}

	}

	@Override
	public TiDBGlobalState generateGlobalState() {
		return new TiDBGlobalState();
	}

	@Override
	public void generateAndTestDatabase(TiDBGlobalState globalState) throws SQLException {
		QueryManager manager = globalState.getManager();
		Connection con = globalState.getConnection();
		String databaseName = globalState.getDatabaseName();
		globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
		StateLogger logger = globalState.getLogger();
		StateToReproduce state = globalState.getState();
		for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
			boolean success = false;
			do {
			Query qt = new TiDBTableGenerator().getQuery(globalState);
			success = manager.execute(qt);
			logger.writeCurrent(state);
			globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
			try {
				logger.getCurrentFileWriter().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.currentFileWriter = null;
			} while (!success);
		}
		globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));

		StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<TiDBGlobalState, Action>(globalState,
				databaseName, Action.values(), TiDBProvider::mapActions, (q) -> {
					if (q.couldAffectSchema()) {
						try {
							globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
						} catch (SQLException e) {
							if (q.getQueryString().contains("CREATE VIEW")) {
								throw new IgnoreMeException(); // TODO: drop view instead
							} else {
								throw new AssertionError(e);
							}
						}
					}
					if (globalState.getSchema().getDatabaseTables().isEmpty()) {
						throw new IgnoreMeException();
					}
				});
		se.executeStatements();
		manager.incrementCreateDatabase();
		List<TestOracle> oracles = globalState.getDmbsSpecificOptions().oracle.stream().map(o -> {
			try {
				return o.create(globalState);
			} catch (SQLException e1) {
				throw new AssertionError(e1);
			}
		}).collect(Collectors.toList());
		CompositeTestOracle oracle = new CompositeTestOracle(oracles);
		
		for (int i = 0; i < globalState.getOptions().getNrQueries(); i++) {
			try {
				oracle.check();
				manager.incrementSelectQueryCount();
			} catch (IgnoreMeException e) {

			}
		}
		try {
			if (globalState.getOptions().logEachSelect()) {
				logger.getCurrentFileWriter().close();
				logger.currentFileWriter = null;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		String url = "jdbc:mysql://127.0.0.1:4001/";
		Connection con = DriverManager.getConnection(url, "root", "");
		state.statements.add(new QueryAdapter("USE test"));
		state.statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
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
		con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:4001/" + databaseName, "root", "");
		return con;
	}

	@Override
	public String getDBMSName() {
		return "TiDB";
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new MySQLStateToReproduce(databaseName);
	}

	@Override
	public TiDBOptions getCommand() {
		return new TiDBOptions();
	}

}
