package sqlancer.tidb;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.MySQLStateToReproduce;
import sqlancer.StatementExecutor;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBTableGenerator;

public class TiDBProvider implements DatabaseProvider<TiDBGlobalState> {

	@FunctionalInterface
	public interface TiDBQueryProvider {

		Query getQuery(TiDBGlobalState globalState) throws SQLException;
	}

	public static enum Action implements AbstractAction<TiDBGlobalState> {
		INSERT(TiDBInsertGenerator::getQuery), //
		ANALYZE_TABLE((g) -> new QueryAdapter("ANALYZE TABLE " + g.getSchema().getRandomTable().getName())),
		TRUNCATE((g) -> new QueryAdapter("TRUNCATE " + g.getSchema().getRandomTable().getName())),
		CREATE_INDEX(TiDBIndexGenerator::getQuery);

		private final TiDBQueryProvider queryProvider;

		private Action(TiDBQueryProvider queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(TiDBGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	public static class TiDBGlobalState extends GlobalState {

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
			return r.getInteger(0, 2);
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
//		TiDBOptions TiDBOptions = new TiDBOptions();
//		JCommander.newBuilder().addObject(TiDBOptions).build().parse(options.getDbmsOptions().split(" "));
//		globalState.setTiDBOptions(TiDBOptions);
//

		for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
			Query qt = new TiDBTableGenerator().getQuery(globalState);
			manager.execute(qt);
			logger.writeCurrent(state);
			globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
			try {
				logger.getCurrentFileWriter().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.currentFileWriter = null;
		}
		globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));

		StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<TiDBGlobalState, Action>(globalState,
				databaseName, Action.values(), TiDBProvider::mapActions, (q) -> {
					if (q.couldAffectSchema()) {
						globalState.setSchema(TiDBSchema.fromConnection(con, databaseName));
					}
					if (globalState.getSchema().getDatabaseTables().isEmpty()) {
						throw new IgnoreMeException();
					}
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
