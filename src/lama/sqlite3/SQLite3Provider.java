package lama.sqlite3;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.DatabaseFacade;
import lama.DatabaseProvider;
import lama.IgnoreMeException;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3AlterTable;
import lama.sqlite3.gen.SQLite3AnalyzeGenerator;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3DeleteGenerator;
import lama.sqlite3.gen.SQLite3DropIndexGenerator;
import lama.sqlite3.gen.SQLite3DropTableGenerator;
import lama.sqlite3.gen.SQLite3ExplainGenerator;
import lama.sqlite3.gen.SQLite3IndexGenerator;
import lama.sqlite3.gen.SQLite3PragmaGenerator;
import lama.sqlite3.gen.SQLite3ReindexGenerator;
import lama.sqlite3.gen.SQLite3RowGenerator;
import lama.sqlite3.gen.SQLite3TableGenerator;
import lama.sqlite3.gen.SQLite3TransactionGenerator;
import lama.sqlite3.gen.SQLite3UpdateGenerator;
import lama.sqlite3.gen.SQLite3VacuumGenerator;
import lama.sqlite3.gen.SQLite3ViewGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3Provider implements DatabaseProvider {

	public static enum Action {
		PRAGMA {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3PragmaGenerator.insertPragma(con, state, r);
			}
		},
		INDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3IndexGenerator.insertIndex(con, state, r);
			}
		},
		INSERT {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				Table randomTable = Randomly.fromList(newSchema.getDatabaseTablesWithoutViews());
				return SQLite3RowGenerator.insertRow(randomTable, con, state, r);
			}

		},
		VACUUM {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3VacuumGenerator.executeVacuum();
			}

		},
		REINDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3ReindexGenerator.executeReindex(con, state, newSchema);
			}

		},
		ANALYZE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3AnalyzeGenerator.generateAnalyze(newSchema);

			}
		},
		DELETE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3DeleteGenerator
						.deleteContent(Randomly.fromList(newSchema.getDatabaseTablesWithoutViews()), con, state, r);
			}
		},
		TRANSACTION_START {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateBeginTransaction(con, state);
			}

		},
		ALTER {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3AlterTable.alterTable(newSchema, con, state, r);
			}

		},
		DROP_INDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3DropIndexGenerator.dropIndex(con, state, newSchema, r);
			}
		},
		UPDATE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3UpdateGenerator.updateRow(newSchema.getRandomTableNoView(), con, state, r);
			}
		},
		ROLLBACK_TRANSACTION() {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
			}
		},
		COMMIT {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateCommit(con, state);
			}

		},
		DROP_TABLE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3DropTableGenerator.dropTable(newSchema);
			}

		},
		EXPLAIN {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3ExplainGenerator.explain(con, (SQLite3StateToReproduce) state, r);
			}
		},
		TARGETED_SELECT {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return new QueryGenerator(con, r).getQueryThatContainsAtLeastOneRow((SQLite3StateToReproduce) state);
			}

		},
		CREATE_VIEW {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3ViewGenerator.generate(con, r, (SQLite3StateToReproduce) state);
			}
		};

		public abstract Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
				throws SQLException;
	}

	public static final int NR_INSERT_ROW_TRIES = 50;
	private static final int NR_QUERIES_PER_TABLE = 10000;
	private static final int MAX_INSERT_ROW_TRIES = 10;
	public static final int EXPRESSION_MAX_DEPTH = 1;
	private SQLite3StateToReproduce state;
	private String databaseName;

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		this.databaseName = databaseName;
		Randomly r = new Randomly();
		SQLite3Schema newSchema = null;
		this.state = (SQLite3StateToReproduce) state;

		addSensiblePragmaDefaults(con);
		int nrTablesToCreate = 1 + Randomly.smallNumber();
		for (int i = 0; i < nrTablesToCreate; i++) {
			newSchema = SQLite3Schema.fromConnection(con);
			assert newSchema.getDatabaseTables().size() == i : newSchema + " " + i;
			String tableName = SQLite3Common.createTableName(i);
			Query tableQuery = SQLite3TableGenerator.createTableStatement(tableName, state, newSchema, r);
			manager.execute(tableQuery);
		}

		newSchema = SQLite3Schema.fromConnection(con);

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case ALTER:
			case DROP_TABLE:
				nrPerformed = r.getInteger(0, 5);
				break;
			case INSERT:
				nrPerformed = NR_INSERT_ROW_TRIES;
				break;
			case EXPLAIN:
				nrPerformed = r.getInteger(0, 200);
				break;
			case TARGETED_SELECT:
				nrPerformed = 0;
				break;
			case CREATE_VIEW:
				nrPerformed = 1;
				break;
			case COMMIT:
			case TRANSACTION_START:
			case INDEX:
			case REINDEX:
			case VACUUM:
			case UPDATE:
			case ANALYZE:
			case PRAGMA:
			case DROP_INDEX:
			case DELETE:
			case ROLLBACK_TRANSACTION:
			default:
				nrPerformed = r.getInteger(1, 30);
				break;
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
			Query query = nextAction.getQuery(newSchema, con, state, r);
			try {
				manager.execute(query);
				if (query.couldAffectSchema()) {
					newSchema = SQLite3Schema.fromConnection(con);
				}
			} catch (IgnoreMeException e) {

			} catch (Throwable t) {
				System.err.println(query.getQueryString());
				throw t;
			}
			total--;
		}
		Query query = SQLite3TransactionGenerator.generateCommit(con, state);
		manager.execute(query);
		// also do an abort for DEFERRABLE INITIALLY DEFERRED
		query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
		manager.execute(query);
		newSchema = SQLite3Schema.fromConnection(con);

		for (Table t : newSchema.getDatabaseTablesWithoutViews()) {
			if (!ensureTableHasRows(con, t, r)) {
				return;
			}
		}

		for (Table t : newSchema.getViews()) {
			if (t.getNrRows() == 0) {
				throw new IgnoreMeException();
			}
		}
		if (Randomly.getBoolean()) {
			SQLite3ReindexGenerator.executeReindex(con, state, newSchema);
		}
		newSchema = SQLite3Schema.fromConnection(con);

		QueryGenerator queryGenerator = new QueryGenerator(con, r);
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
			try {
				queryGenerator.generateAndCheckQuery(this.state, logger, options);
			} catch (IgnoreMeException e) {

			}
			manager.incrementSelectQueryCount();
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

	private void addSensiblePragmaDefaults(Connection con) throws SQLException {
		List<String> defaultSettings = Arrays.asList("PRAGMA cache_size = 10000;", "PRAGMA temp_store=MEMORY;",
				"PRAGMA synchronous=off;");
		for (String s : defaultSettings) {
			Query q = new QueryAdapter(s);
			state.statements.add(q);
			q.execute(con);
		}
	}

	private boolean ensureTableHasRows(Connection con, Table randomTable, Randomly r)
			throws AssertionError, SQLException {
		int nrRows;
		int counter = MAX_INSERT_ROW_TRIES;
		do {
			try {
				Query q = SQLite3RowGenerator.insertRow(randomTable, con, state, r);
				state.statements.add(q);
				q.execute(con);

			} catch (SQLException e) {
				if (!QueryGenerator.shouldIgnoreException(e)) {
					throw new AssertionError(e);
				}
			}
			nrRows = randomTable.getNrRows();
		} while (nrRows == 0 && counter-- != 0);
		return nrRows != 0;
	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		return DatabaseFacade.createDatabase(databaseName);
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "sqlite3";
	}

	@Override
	public String toString() {
		return String.format("SQLite3Provider [database: %s]", databaseName);
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		SQLite3StateToReproduce specificState = (SQLite3StateToReproduce) state;
		if (specificState.getRandomRowValues() != null) {
			List<Column> columnList = specificState.getRandomRowValues().keySet().stream().collect(Collectors.toList());
			List<Table> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
					.collect(Collectors.toList());
			for (Table t : tableList) {
				sb.append("-- " + t.getName() + "\n");
				List<Column> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
						.collect(Collectors.toList());
				for (Column c : columnsForTable) {
					sb.append("--\t");
					sb.append(c);
					sb.append("=");
					sb.append(specificState.getRandomRowValues().get(c));
					sb.append("\n");
				}
			}
			sb.append("-- expected values: \n");
			String asExpectedValues = "-- "
					+ SQLite3Visitor.asExpectedValues(specificState.getWhereClause()).replace("\n", "\n-- ");
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
		return new SQLite3StateToReproduce(databaseName);
	}

	@Override
	public Query checkIfRowIsStillContained(StateToReproduce state) {
		String checkRowIsInside = "SELECT " + state.queryTargetedColumnsString + " FROM "
				+ state.queryTargetedTablesString + " INTERSECT SELECT " + state.values;
		return new QueryAdapter(checkRowIsInside);
	}

}
