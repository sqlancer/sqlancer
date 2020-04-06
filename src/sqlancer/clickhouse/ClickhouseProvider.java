package sqlancer.clickhouse;

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
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StatementExecutor;
import sqlancer.clickhouse.ClickhouseProvider.ClickhouseGlobalState;
import sqlancer.clickhouse.gen.ClickhouseInsertGenerator;
import sqlancer.clickhouse.gen.ClickhouseTableGenerator;
import sqlancer.clickhouse.test.ClickhouseQueryPartitioningWhereTester;

public class ClickhouseProvider implements DatabaseProvider<ClickhouseGlobalState, ClickhouseOptions> {

	public static enum Action implements AbstractAction<ClickhouseGlobalState> {

		INSERT(ClickhouseInsertGenerator::getQuery);

		private final QueryProvider<ClickhouseGlobalState> queryProvider;

		private Action(QueryProvider<ClickhouseGlobalState> queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(ClickhouseGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	private static int mapActions(ClickhouseGlobalState globalState, Action a) {
		Randomly r = globalState.getRandomly();
		switch (a) {
		case INSERT:
			return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
		default:
			throw new AssertionError(a);
		}
	}

	public static class ClickhouseGlobalState extends GlobalState<ClickhouseOptions> {

		private ClickhouseSchema schema;

		public void setSchema(ClickhouseSchema schema) {
			this.schema = schema;
		}

		public ClickhouseSchema getSchema() {
			return schema;
		}

	}

	@Override
	public void generateAndTestDatabase(ClickhouseGlobalState globalState) throws SQLException {
		StateLogger logger = globalState.getLogger();
		QueryManager manager = globalState.getManager();
		globalState.setSchema(
				ClickhouseSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
		for (int i = 0; i < Randomly.fromOptions(1); i++) {
			boolean success = false;
			do {
				Query qt = new ClickhouseTableGenerator().getQuery(globalState);
				success = manager.execute(qt);
				logger.writeCurrent(globalState.getState());
				globalState.setSchema(
						ClickhouseSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
				try {
					logger.getCurrentFileWriter().close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.currentFileWriter = null;
			} while (!success);
		}

		StatementExecutor<ClickhouseGlobalState, Action> se = new StatementExecutor<ClickhouseGlobalState, Action>(
				globalState, globalState.getDatabaseName(), Action.values(), ClickhouseProvider::mapActions, (q) -> {
					if (q.couldAffectSchema()) {
						globalState.setSchema(ClickhouseSchema.fromConnection(globalState.getConnection(),
								globalState.getDatabaseName()));
					}
					if (globalState.getSchema().getDatabaseTables().isEmpty()) {
						throw new IgnoreMeException();
					}
				});
		se.executeStatements();
		manager.incrementCreateDatabase();
		
		ClickhouseQueryPartitioningWhereTester oracle = new ClickhouseQueryPartitioningWhereTester(globalState);
		
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
		String url = "jdbc:clickhouse://localhost:8123/test";
		Connection con = DriverManager.getConnection(url, "", "password");
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
		con = DriverManager.getConnection("jdbc:clickhouse://localhost:8123/" + databaseName, "", "password");
		return con;
	}

	@Override
	public String getDBMSName() {
		return "Clickhouse";
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new StateToReproduce(databaseName);
	}

	@Override
	public ClickhouseGlobalState generateGlobalState() {
		return new ClickhouseGlobalState();
	}

	@Override
	public ClickhouseOptions getCommand() {
		return new ClickhouseOptions();
	}
}
