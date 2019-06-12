package lama.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lama.DatabaseFacade;
import lama.DatabaseProvider;
import lama.Main;
import lama.Main.StateLogger;
import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.gen.QueryGenerator;
import lama.sqlite3.gen.SQLite3AlterTable;
import lama.sqlite3.gen.SQLite3AnalyzeGenerator;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3DeleteGenerator;
import lama.sqlite3.gen.SQLite3DropIndexGenerator;
import lama.sqlite3.gen.SQLite3IndexGenerator;
import lama.sqlite3.gen.SQLite3PragmaGenerator;
import lama.sqlite3.gen.SQLite3ReindexGenerator;
import lama.sqlite3.gen.SQLite3RowGenerator;
import lama.sqlite3.gen.SQLite3TableGenerator;
import lama.sqlite3.gen.SQLite3TransactionGenerator;
import lama.sqlite3.gen.SQLite3UpdateGenerator;
import lama.sqlite3.gen.SQLite3VacuumGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3Provider implements DatabaseProvider {

	private enum Action {
		PRAGMA, INDEX, INSERT, VACUUM, REINDEX, ANALYZE, DELETE, TRANSACTION_START, ALTER, DROP_INDEX, UPDATE,
		ROLLBACK_TRANSACTION, COMMIT;
	}

	private static final int NR_QUERIES_PER_TABLE = 1000;
	private static final int MAX_INSERT_ROW_TRIES = 1;
	public static final int EXPRESSION_MAX_DEPTH = 3;
	private StateToReproduce state;

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger) throws SQLException {
		Randomly r = new Randomly();
		state = new StateToReproduce(databaseName);
		SQLite3Schema newSchema = null;

		addSensiblePragmaDefaults(con);
		int nrTablesToCreate = 1 + Randomly.smallNumber();
		for (int i = 0; i < nrTablesToCreate; i++) {
			newSchema = SQLite3Schema.fromConnection(con);
			assert newSchema.getDatabaseTables().size() == i : newSchema + " " + i;
			String tableName = SQLite3Common.createTableName(i);
			Query tableQuery = SQLite3TableGenerator.createTableStatement(tableName, state, newSchema, r);
			state.statements.add(tableQuery);
			tableQuery.execute(con);
		}

		newSchema = SQLite3Schema.fromConnection(con);

		java.sql.DatabaseMetaData meta = con.getMetaData();
		state.databaseVersion = meta.getDatabaseProductVersion();

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case ALTER:
				nrPerformed = r.getInteger(0, 5);
				break;
			case INSERT:
				nrPerformed = Main.NR_INSERT_ROW_TRIES;
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
			Query query;
			switch (nextAction) {
			case ALTER:
				query = SQLite3AlterTable.alterTable(newSchema, con, state, r);
				break;
			case ROLLBACK_TRANSACTION:
				query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
				break;
			case UPDATE:
				query = SQLite3UpdateGenerator.updateRow(newSchema.getRandomTable(), con, state, r);
				break;
			case TRANSACTION_START:
				query = SQLite3TransactionGenerator.generateBeginTransaction(con, state);
				break;
			case INDEX:
				query = SQLite3IndexGenerator.insertIndex(con, state, r);
				break;
			case DROP_INDEX:
				query = SQLite3DropIndexGenerator.dropIndex(con, state, newSchema, r);
				break;
			case DELETE:
				query = SQLite3DeleteGenerator.deleteContent(Randomly.fromList(newSchema.getDatabaseTables()), con,
						state, r);
				break;
			case INSERT:
				Table randomTable = Randomly.fromList(newSchema.getDatabaseTables());
				query = SQLite3RowGenerator.insertRow(randomTable, con, state, r);
				break;
			case PRAGMA:
				query = SQLite3PragmaGenerator.insertPragma(con, state, r);
				break;
			case REINDEX:
				query = SQLite3ReindexGenerator.executeReindex(con, state);
				break;
			case COMMIT:
				query = SQLite3TransactionGenerator.generateCommit(con, state);
				break;
			case VACUUM:
				query = SQLite3VacuumGenerator.executeVacuum(con, state);
				break;
			case ANALYZE:
				query = SQLite3AnalyzeGenerator.generateAnalyze();
				break;
			default:
				throw new AssertionError(nextAction);
			}
			state.statements.add(query);
			try {
				query.execute(con);
				if (query.couldAffectSchema()) {
					newSchema = SQLite3Schema.fromConnection(con);
				}
			} catch (Throwable t) {
				System.err.println(query.getQueryString());
				throw t;
			}
			total--;
		}
		Query query = SQLite3TransactionGenerator.generateCommit(con, state);
		query.execute(con);
		// also do an abort for DEFERRABLE INITIALLY DEFERRED
		state.statements.add(query);

		query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
		query.execute(con);
		state.statements.add(query);
		newSchema = SQLite3Schema.fromConnection(con);

		for (Table t : newSchema.getDatabaseTables()) {
			if (!ensureTableHasRows(con, t, r)) {
				return;
			}
		}
		if (Randomly.getBoolean()) {
			SQLite3ReindexGenerator.executeReindex(con, state);
		}
		newSchema = SQLite3Schema.fromConnection(con);

		// logger.writeCurrent(state);
		QueryGenerator queryGenerator = new QueryGenerator(con, r);
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {
			queryGenerator.generateAndCheckQuery(state, logger);
			Main.nrQueries.addAndGet(1);
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
			nrRows = SQLite3Helper.getNrRows(con, randomTable);
		} while (nrRows == 0 && counter-- != 0);
		return nrRows != 0;
	}

	@Override
	public Connection createDatabase(String databaseName) throws SQLException {
		return DatabaseFacade.createDatabase(databaseName);
	}
}
