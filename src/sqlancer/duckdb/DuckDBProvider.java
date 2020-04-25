package sqlancer.duckdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

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
import sqlancer.TestOracle;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.gen.DuckDBDeleteGenerator;
import sqlancer.duckdb.gen.DuckDBIndexGenerator;
import sqlancer.duckdb.gen.DuckDBInsertGenerator;
import sqlancer.duckdb.gen.DuckDBRandomQuerySynthesizer;
import sqlancer.duckdb.gen.DuckDBTableGenerator;
import sqlancer.duckdb.gen.DuckDBUpdateGenerator;
import sqlancer.duckdb.gen.DuckDBViewGenerator;

public class DuckDBProvider implements DatabaseProvider<DuckDBGlobalState, DuckDBOptions> {

	public static enum Action implements AbstractAction<DuckDBGlobalState> {

		INSERT(DuckDBInsertGenerator::getQuery),
		CREATE_INDEX(DuckDBIndexGenerator::getQuery),
		VACUUM((g) -> new QueryAdapter("VACUUM;")),
		ANALYZE((g) -> new QueryAdapter("ANALYZE;")),
		DELETE(DuckDBDeleteGenerator::generate),
		UPDATE(DuckDBUpdateGenerator::getQuery),
		CREATE_VIEW(DuckDBViewGenerator::generate),
		EXPLAIN((g) -> {
			Set<String> errors = new HashSet<>();
			DuckDBErrors.addExpressionErrors(errors);;
			DuckDBErrors.addGroupByErrors(errors);
			return new QueryAdapter("EXPLAIN " + DuckDBToStringVisitor.asString(DuckDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)), errors);	
		});


		private final QueryProvider<DuckDBGlobalState> queryProvider;

		private Action(QueryProvider<DuckDBGlobalState> queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(DuckDBGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	private static int mapActions(DuckDBGlobalState globalState, Action a) {
		Randomly r = globalState.getRandomly();
		switch (a) {
		case INSERT:
			return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
		case CREATE_INDEX:
			if (!globalState.getDmbsSpecificOptions().testIndexes) {
				return 0;
			}
			// fall through
		case UPDATE:
			return r.getInteger(0, 4);
		case VACUUM: // seems to be ignored
		case ANALYZE:  // seems to be ignored
		case DELETE:
		case EXPLAIN:
		case CREATE_VIEW:
			return r.getInteger(0, 2);
		default:
			throw new AssertionError(a);
		}
	}

	public static class DuckDBGlobalState extends GlobalState<DuckDBOptions> {

		private DuckDBSchema schema;

		public void setSchema(DuckDBSchema schema) {
			this.schema = schema;
		}

		public DuckDBSchema getSchema() {
			return schema;
		}

	}

	@Override
	public void generateAndTestDatabase(DuckDBGlobalState globalState) throws SQLException {
		StateLogger logger = globalState.getLogger();
		QueryManager manager = globalState.getManager();
		globalState.setSchema(DuckDBSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
		for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
			boolean success = false;
			do {
				Query qt = new DuckDBTableGenerator().getQuery(globalState);
				if (globalState.getOptions().logEachSelect()) {
					globalState.getLogger().writeCurrent(qt.getQueryString());
				}
				success = manager.execute(qt);
				globalState.setSchema(
						DuckDBSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName()));
			} while (!success);
		}
		if (globalState.getSchema().getDatabaseTables().size() == 0) {
			throw new IgnoreMeException(); // TODO
		}
		StatementExecutor<DuckDBGlobalState, Action> se = new StatementExecutor<DuckDBGlobalState, Action>(globalState,
				globalState.getDatabaseName(), Action.values(), DuckDBProvider::mapActions, (q) -> {
					if (q.couldAffectSchema()) {
						globalState.setSchema(DuckDBSchema.fromConnection(globalState.getConnection(),
								globalState.getDatabaseName()));
					}
					if (globalState.getSchema().getDatabaseTables().isEmpty()) {
						throw new IgnoreMeException();
					}
				});
		se.executeStatements();
		manager.incrementCreateDatabase();

		TestOracle oracle = globalState.getDmbsSpecificOptions().oracle.get(0).create(globalState);

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
		globalState.getConnection().close();
	}

	@Override
	public Connection createDatabase(GlobalState<?> globalState) throws SQLException {
		try {
			Class.forName("nl.cwi.da.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String url = "jdbc:duckdb:";
		Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
				globalState.getOptions().getPassword());
		return con;
	}

	@Override
	public String getDBMSName() {
		return "DuckDB";
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new StateToReproduce(databaseName);
	}

	@Override
	public DuckDBGlobalState generateGlobalState() {
		return new DuckDBGlobalState();
	}

	@Override
	public DuckDBOptions getCommand() {
		return new DuckDBOptions();
	}
}
