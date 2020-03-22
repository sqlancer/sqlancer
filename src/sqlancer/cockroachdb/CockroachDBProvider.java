package sqlancer.cockroachdb;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Main.QueryManager;
import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.CockroachDBStateToReproduce;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.gen.CockroachDBCreateStatisticsGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDeleteGenerator;
import sqlancer.cockroachdb.gen.CockroachDBIndexGenerator;
import sqlancer.cockroachdb.gen.CockroachDBInsertGenerator;
import sqlancer.cockroachdb.gen.CockroachDBRandomQuerySynthesizer;
import sqlancer.cockroachdb.gen.CockroachDBSetClusterSettingGenerator;
import sqlancer.cockroachdb.gen.CockroachDBSetSessionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBShowGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTruncateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBUpdateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBViewGenerator;
import sqlancer.cockroachdb.gen.RockroachDBCommentOnGenerator;

public class CockroachDBProvider implements DatabaseProvider {

	@FunctionalInterface
	public interface CockroachDBQueryProvider {

		Query getQuery(CockroachDBGlobalState globalState) throws SQLException;
	}

	public static enum Action {
		INSERT(CockroachDBInsertGenerator::insert), //
		TRUNCATE(CockroachDBTruncateGenerator::truncate), //
		CREATE_STATISTICS(CockroachDBCreateStatisticsGenerator::create), //
		SET_SESSION(CockroachDBSetSessionGenerator::create), //
		CREATE_INDEX(CockroachDBIndexGenerator::create), //
		UPDATE(CockroachDBUpdateGenerator::gen), //
		CREATE_VIEW(CockroachDBViewGenerator::generate), //
		SET_CLUSTER_SETTING(CockroachDBSetClusterSettingGenerator::create), DELETE(CockroachDBDeleteGenerator::delete),
		COMMENT_ON(RockroachDBCommentOnGenerator::comment), SHOW(CockroachDBShowGenerator::show), TRANSACTION((g) -> {
			String s = Randomly.fromOptions("BEGIN", "ROLLBACK", "COMMIT");
			return new QueryAdapter(s, Arrays.asList("there is no transaction in progress",
					"there is already a transaction in progress", "current transaction is aborted"));
		}), EXPLAIN((g) -> {
			new CockroachDBRandomQuerySynthesizer();
			StringBuilder sb = new StringBuilder("EXPLAIN ");
			Set<String> errors = new HashSet<>();
			if (Randomly.getBoolean()) {
				sb.append("(");
				sb.append(Randomly.nonEmptySubset("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC").stream()
						.collect(Collectors.joining(", ")));
				sb.append(") ");
				errors.add("cannot set EXPLAIN mode more than once");
				errors.add("unable to vectorize execution plan");
				errors.add("unsupported type");
				errors.add("vectorize is set to 'off'");
			}
			sb.append(CockroachDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1));
			CockroachDBErrors.addExpressionErrors(errors);
			return new QueryAdapter(sb.toString(), errors);
		}),
		SCRUB((g) -> new QueryAdapter(
				"EXPERIMENTAL SCRUB table " + g.getSchema().getRandomTable(t -> !t.isView()).getName(),
				Arrays.asList()));

		private final CockroachDBQueryProvider queryProvider;

		private Action(CockroachDBQueryProvider queryProvider) {
			this.queryProvider = queryProvider;
		}

		public Query getQuery(CockroachDBGlobalState state) throws SQLException {
			return queryProvider.getQuery(state);
		}
	}

	public static class CockroachDBGlobalState {

		private Connection con;
		private CockroachDBSchema schema;
		private Randomly r;
		private MainOptions options;
		private StateLogger logger;
		private StateToReproduce state;
		private CockroachDBOptions cockroachdbOptions;

		public void setConnection(Connection con) {
			this.con = con;
		}

		public Connection getConnection() {
			return con;
		}

		public void setSchema(CockroachDBSchema schema) {
			this.schema = schema;
		}

		public CockroachDBSchema getSchema() {
			return schema;
		}

		public void setRandomly(Randomly r) {
			this.r = r;
		}

		public Randomly getRandomly() {
			return r;
		}

		public MainOptions getOptions() {
			return options;
		}

		public void setMainOptions(MainOptions options) {
			this.options = options;
		}

		public void setStateLogger(StateLogger logger) {
			this.logger = logger;
		}

		public StateLogger getLogger() {
			return logger;
		}

		public void setState(StateToReproduce state) {
			this.state = state;
		}

		public StateToReproduce getState() {
			return state;
		}

		public void setCockroachDBOptions(CockroachDBOptions cockroachdbOptions) {
			this.cockroachdbOptions = cockroachdbOptions;
		}

		public CockroachDBOptions getCockroachdbOptions() {
			return cockroachdbOptions;
		}
		
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		Randomly r = new Randomly();
		CockroachDBGlobalState globalState = new CockroachDBGlobalState();
		globalState.setConnection(con);
		globalState.setSchema(CockroachDBSchema.fromConnection(con, databaseName));
		globalState.setRandomly(r);
		globalState.setMainOptions(options);
		globalState.setStateLogger(logger);
		globalState.setState(state);
		CockroachDBOptions cockroachdbOptions = new CockroachDBOptions();
		JCommander.newBuilder().addObject(cockroachdbOptions).build().parse(options.getDbmsOptions().split(" "));
		globalState.setCockroachDBOptions(cockroachdbOptions);
		
		manager.execute(new QueryAdapter("SET CLUSTER SETTING debug.panic_on_failed_assertions = true;"));
		manager.execute(new QueryAdapter("SET CLUSTER SETTING diagnostics.reporting.enabled	 = false;"));
		manager.execute(
				new QueryAdapter("SET CLUSTER SETTING diagnostics.reporting.send_crash_reports		 = false;"));

		for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
			boolean success = false;
			do {
				try {
					Query q = CockroachDBTableGenerator.generate(globalState);
					success = manager.execute(q);
					logger.writeCurrent(state);
					try {
						logger.getCurrentFileWriter().close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					logger.currentFileWriter = null;
				} catch (IgnoreMeException e) {

				}
			} while (!success);
			globalState.setSchema(CockroachDBSchema.fromConnection(con, databaseName));
		}
		logger.writeCurrent(state);

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case INSERT:
				nrPerformed = r.getInteger(0, options.getMaxNumberInserts());
				break;
			case UPDATE:
				nrPerformed = r.getInteger(0, 3);
				break;
			case EXPLAIN:
				nrPerformed = r.getInteger(0, 10);
				break;
			case SHOW:
			case COMMENT_ON:
			case TRUNCATE:
			case DELETE:
			case CREATE_STATISTICS:
				nrPerformed = r.getInteger(0, 2);
				break;
			case CREATE_VIEW:
				nrPerformed = r.getInteger(0, 2);
				break;
			case SET_SESSION:
			case SET_CLUSTER_SETTING:
				nrPerformed = r.getInteger(0, 3);
				break;
			case CREATE_INDEX:
				nrPerformed = r.getInteger(0, 10);
				break;
			case SCRUB:
				nrPerformed = 1;
				break;
			case TRANSACTION:
				nrPerformed = 0; // r.getInteger(0, 0);
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
			Query query = null;
			try {
				boolean success;
				int nrTries = 0;
				do {
					query = nextAction.getQuery(globalState);
					if (options.logEachSelect()) {
						logger.writeCurrent(query.getQueryString());
					}
					success = manager.execute(query);
				} while (!success && nrTries++ < 1000);
			} catch (IgnoreMeException e) {

			}
			if (query != null && query.couldAffectSchema()) {
				globalState.setSchema(CockroachDBSchema.fromConnection(con, databaseName));
				if (globalState.getSchema().getDatabaseTables().isEmpty()) {
					throw new IgnoreMeException();
				}
			}
			total--;
		}
		manager.incrementCreateDatabase();
		if (Randomly.getBoolean()) {
			manager.execute(new QueryAdapter("SET vectorize=on;"));
		}
		TestOracle oracle = globalState.getCockroachdbOptions().oracle.create(globalState);
		for (int i = 0; i < options.getNrQueries(); i++) {
			try {
				oracle.check();
				manager.incrementSelectQueryCount();
			} catch (IgnoreMeException e) {

			}
		}
		try {
			if (options.logEachSelect()) {
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
		String url = "jdbc:postgresql://localhost:26257/test";
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
		con = DriverManager.getConnection("jdbc:postgresql://localhost:26257/" + databaseName, "root", "");
		return con;
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "cockroachdb";
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		// TODO Auto-generated method stub

	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new CockroachDBStateToReproduce(databaseName);
	}
}
