package lama.tdengine;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import lama.StateToReproduce.TDEngineStateToReproduce;
import lama.sqlite3.gen.SQLite3Common;
import lama.tdengine.TDEngineSchema.TDEngineColumn;
import lama.tdengine.TDEngineSchema.TDEngineTable;
import lama.tdengine.expr.TDEngineExpression;
import lama.tdengine.gen.TDEngineAlterTableGenerator;
import lama.tdengine.gen.TDEngineQueryGenerator;
import lama.tdengine.gen.TDEngineRowGenerator;
import lama.tdengine.gen.TDEngineTableGenerator;

public class TDEngineProvider implements DatabaseProvider {

	public static enum Action {

		INSERT {

			@Override
			public Query getQuery(TDEngineSchema newSchema, Connection con, Randomly r) throws SQLException {
				TDEngineTable randomTable=Randomly.fromList(newSchema.getDatabaseTables());return TDEngineRowGenerator.insertRow(randomTable,con,r);
			}

		},
		ALTER_TABLE {

			@Override
			public Query getQuery(TDEngineSchema newSchema, Connection con, Randomly r) throws SQLException {
				return TDEngineAlterTableGenerator.gen(newSchema.getRandomTable(),con,r);
			}

		},
		DROP_TABLE {
		@Override
			public Query getQuery(TDEngineSchema newSchema, Connection con, Randomly r) throws SQLException {
				StringBuilder sb = new StringBuilder();
				if (newSchema.getDatabaseTables().size() == 1) {
					throw new IgnoreMeException();
				}
				sb.append("DROP TABLE ");
				sb.append(newSchema.getRandomTable().getName());
				return new QueryAdapter(sb.toString()) {
					public boolean couldAffectSchema() {return true; };
				};
			}
		};

		public abstract Query getQuery(TDEngineSchema newSchema, Connection con, Randomly r) throws SQLException;
	}

	public static final int NR_INSERT_ROW_TRIES = 10000;
	private static final int NR_QUERIES_PER_TABLE = 50000;
	public static final int EXPRESSION_MAX_DEPTH = 0;
	private TDEngineStateToReproduce state;
	private String databaseName;

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		this.databaseName = databaseName;
		Randomly r = new Randomly();
		TDEngineSchema newSchema = null;
		this.state = (TDEngineStateToReproduce) state;

		int nrTablesToCreate = 1 + Randomly.smallNumber();
		newSchema = TDEngineSchema.fromConnection(con);
		while (newSchema.getDatabaseTables().isEmpty()) {
			for (int i = 0; i < nrTablesToCreate; i++) {
				newSchema = TDEngineSchema.fromConnection(con);
				String tableName = SQLite3Common.createTableName(i);
				Query tableQuery = TDEngineTableGenerator.generate(tableName);
				manager.execute(tableQuery);
			}
			newSchema = TDEngineSchema.fromConnection(con);
		}

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case INSERT:
				nrPerformed = r.getInteger(0, 1000);
				break;
			case DROP_TABLE:
				nrPerformed = r.getInteger(0, 3);
				break;
			case ALTER_TABLE:
				nrPerformed = r.getInteger(0, 5);
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
			Query query = nextAction.getQuery(newSchema, con, r);
			try {
				manager.execute(query);
				if (query.couldAffectSchema()) {
					newSchema = TDEngineSchema.fromConnection(con);
				}
			} catch (IgnoreMeException e) {

			}
			total--;
		}
		TDEngineQueryGenerator queryGenerator = new TDEngineQueryGenerator(con, r, newSchema);
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
			queryGenerator.generateAndCheckQuery(this.state, logger, options);
			manager.incrementSelectQueryCount();
			if (options.logEachSelect()) {
				try {
					logger.getCurrentFileWriter().close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.currentFileWriter = null;
			}
		}
	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		try {
			Class.forName("com.taosdata.jdbc.TSDBDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		databaseName = databaseName + "asdf";

		String jdbcUrl = "jdbc:TAOS://127.0.0.1:0/";
		Properties conProps = new Properties();
		Connection con = DriverManager.getConnection(jdbcUrl, conProps);
		try (Statement s = con.createStatement()) {
			String drop = "DROP DATABASE IF EXISTS " + databaseName;
			state.statements.add(new QueryAdapter(drop));
			s.execute(drop);
			while (true) {
				try {
					Query create = createDatabase(databaseName);
					s.execute(create.getQueryString());
					state.statements.add(create);
					break;
				} catch (Exception e) {

				}
			}
			state.statements.add(new QueryAdapter("USE " + databaseName));
		}
		con.close();
		Connection newConnection = DriverManager.getConnection(jdbcUrl + databaseName);
		return newConnection;
	}

	enum DatabaseAction {
		DAYS, KEEP, PRECISION, ROWS, COMP, CTIME, CLOG, /* TABLES, CACHE, */ TBLOCKS, ABLOCKS
	}

	private Query createDatabase(String databaseName) {
		List<String> errors = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE DATABASE " + databaseName);
		List<DatabaseAction> actions = Randomly.subset(DatabaseAction.values());
		errors.add("invalid option");
		for (DatabaseAction a : actions) {
			sb.append(" ");
			switch (a) {
			case DAYS:
				sb.append("days ");
				sb.append(Randomly.getNotCachedInteger(1, 100));
				break;
			case KEEP:
				sb.append("keep ");
				sb.append(Randomly.getNotCachedInteger(1, 100));
				break;
			case ROWS:
				sb.append("rows ");
				sb.append(Randomly.getNonCachedInteger());
				break;
			case COMP:
				sb.append("comp ");
				sb.append(Randomly.fromOptions("0", "1", "2"));
				break;
			case CTIME:
				sb.append("ctime ");
				sb.append(Randomly.getNotCachedInteger(0, 100));
				break;
			case CLOG:
				sb.append("clog ");
				sb.append(Randomly.fromOptions("0", "1"));
				break;
//			case TABLES:
//				sb.append("tables ");
//				sb.append(Randomly.getNotCachedInteger(0, 100));
//				break;
//			case CACHE:
//				sb.append("cache ");
//				sb.append(Randomly.getPositiveOrZeroNonCachedInteger());
//				break;
			case TBLOCKS:
				sb.append("tblocks ");
				sb.append(Randomly.getPositiveOrZeroNonCachedInteger());
				break;
			case ABLOCKS:
				sb.append("ablocks ");
				sb.append(Randomly.getPositiveOrZeroNonCachedInteger());
				break;
			case PRECISION:
				sb.append("precision ");
				sb.append('"');
				sb.append(Randomly.fromOptions("ms", "us"));
				sb.append('"');
				break;
			default:
				throw new AssertionError(a);
			}
		}
		return new QueryAdapter(sb.toString(), errors);
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "tdengine";
	}

	@Override
	public String toString() {
		return String.format("TDEngineProvider [database: %s]", databaseName);
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		TDEngineStateToReproduce specificState = (TDEngineStateToReproduce) state;
		if (specificState.getRandomRowValues() != null) {
			List<TDEngineColumn> columnList = specificState.getRandomRowValues().keySet().stream()
					.collect(Collectors.toList());
			List<TDEngineTable> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
					.collect(Collectors.toList());
			for (TDEngineTable t : tableList) {
				sb.append("-- " + t.getName() + "\n");
				List<TDEngineColumn> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
						.collect(Collectors.toList());
				for (TDEngineColumn c : columnsForTable) {
					sb.append("--\t");
					sb.append(c);
					sb.append("=");
					sb.append(specificState.getRandomRowValues().get(c));
					sb.append("\n");
				}
			}
			sb.append("-- expected values: \n");
			TDEngineExpression whereClause = specificState.getWhereClause();
			assert whereClause != null;
			String asExpectedValues = "-- "
					+ TDEngineVisitor.asExpectedValues(whereClause).replace("\n", "\n-- ");
			sb.append(asExpectedValues);
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
		return new TDEngineStateToReproduce(databaseName);
	}

}
