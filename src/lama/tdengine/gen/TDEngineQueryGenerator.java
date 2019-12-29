package lama.tdengine.gen;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lama.Main;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.TDEngineStateToReproduce;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.tdengine.TDEngineSchema;
import lama.tdengine.TDEngineSchema.TDEngineColumn;
import lama.tdengine.TDEngineSchema.TDEngineRowValue;
import lama.tdengine.TDEngineSchema.TDEngineTable;
import lama.tdengine.TDEngineSchema.TDEngineTables;
import lama.tdengine.TDEngineToStringVisitor;
import lama.tdengine.TDEngineVisitor;
import lama.tdengine.expr.TDEngineColumnName;
import lama.tdengine.expr.TDEngineConstant;
import lama.tdengine.expr.TDEngineExpression;
import lama.tdengine.expr.TDEngineSelectStatement;

public class TDEngineQueryGenerator {

	private final Connection database;
	private final TDEngineSchema s;
	private final Randomly r;
	private TDEngineStateToReproduce state;
	private TDEngineRowValue rw;
	private List<TDEngineColumn> fetchTDEngineColumns;
	private final List<String> errors = new ArrayList<>();
	private List<TDEngineExpression> colExpressions;
	private MainOptions options;

	public TDEngineQueryGenerator(Connection con, Randomly r, TDEngineSchema s, MainOptions options) throws SQLException {
		this.database = con;
		this.r = r;
		this.s = s;
		this.options = options;
	}

	public void generateAndCheckQuery(TDEngineStateToReproduce state, StateLogger logger, MainOptions options)
			throws SQLException {
		Query query = getQueryThatContainsAtLeastOneRow(state);
		if (options.logEachSelect()) {
			logger.writeCurrent(query.getQueryString());
		}
		boolean isContainedIn = isContainedIn(query);
		if (!isContainedIn) {
			throw new Main.ReduceMeException();
		}
	}

	public Query getQueryThatContainsAtLeastOneRow(TDEngineStateToReproduce state) throws SQLException {
		TDEngineSelectStatement selectStatement = getQuery(state);
		TDEngineToStringVisitor visitor = new TDEngineToStringVisitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		return new QueryAdapter(queryString, errors);
	}

	TDEngineSelectStatement getQuery(TDEngineStateToReproduce state) throws SQLException {
		this.state = state;
		TDEngineTable table = s.getRandomTable();
		TDEngineTables randomFromTables = new TDEngineTables(Arrays.asList(table));

		state.queryTargetedTablesString = randomFromTables.tableNamesAsString();
		TDEngineSelectStatement selectStatement = new TDEngineSelectStatement();
		List<TDEngineColumn> columns = randomFromTables.getColumns();
		rw = randomFromTables.getRandomRowValue(database, state);

		List<Join> joinStatements = new ArrayList<>();
		selectStatement.setFromTable(table);

		fetchTDEngineColumns = Randomly.nonEmptySubset(table.getColumns());
		colExpressions = new ArrayList<>();
		for (TDEngineColumn c : fetchTDEngineColumns) {
			TDEngineConstant val = rw.getValues().get(c);
			assert val != null;
			TDEngineExpression colName = new TDEngineColumnName(c, val);
			colExpressions.add(colName);
		}
		selectStatement.setFetchColumns(colExpressions);
		state.queryTargetedColumnsString = fetchTDEngineColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		TDEngineExpression whereClause = generateWhereClauseThatContainsRowValue(columns);
		selectStatement.setWhereClause(whereClause);
		state.whereClause = selectStatement;
		if (Randomly.getBoolean()) {
			TDEngineExpression limitClause = generateLimit((long) (Math.pow(options.getMaxNumberInserts(),
					joinStatements.size() + randomFromTables.getTables().size())));
			selectStatement.setLimitClause(limitClause);
			if (Randomly.getBoolean()) {
				TDEngineExpression offsetClause = generateOffset();
				selectStatement.setOffsetClause(offsetClause);
			}
		}
//		TDEngineExpression orderBy = generateOrderBy(columns);
		selectStatement.setOrderByClause(null);
		return selectStatement;
	}

	private TDEngineExpression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return TDEngineConstant.createIntConstant(0);
		} else {
			return null;
		}
	}

	private boolean isContainedIn(Query query) throws SQLException {
		Statement createStatement;
		createStatement = database.createStatement();

		StringBuilder sb = new StringBuilder();
//		sb.append("SELECT ");
//		addExpectedValues(sb);
		StringBuilder sb2 = new StringBuilder();
		addExpectedValues(sb2);
		state.values = sb2.toString();
//		sb.append(" INTERSECT SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
		sb.append(query.getQueryString());
//		sb.append(")");
		String resultingQueryString = sb.toString();
		state.queryString = resultingQueryString;
		Query finalQuery = new QueryAdapter(resultingQueryString, query.getExpectedErrors());
		try (ResultSet result = createStatement.executeQuery(finalQuery.getQueryString())) {
			boolean isContainedIn = !result.isClosed();
			createStatement.close();
			return isContainedIn;
		} catch (SQLException e) {
			for (String exp : finalQuery.getExpectedErrors()) {
				if (e.getMessage().contains(exp)) {
					return true;
				}
			}
			throw e;
		}
	}

	private void addExpectedValues(StringBuilder sb) {
		for (int i = 0; i < colExpressions.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			TDEngineConstant expectedValue = colExpressions.get(i).getExpectedValue();
			sb.append(TDEngineVisitor.asString(expectedValue));
		}
	}

//	public TDEngineExpression generateOrderBy(List<TDEngineColumn> columns) {
//		// TODO: generate column
//		TDEngineExpression expr = new TDEngineExpressionGenerator(r).setColumns(columns).getRandomExpression();
//		TDOrdering order = Randomly.fromOptions(TDOrdering.ASC, TDOrdering.DESC);
//		return new TDEngineOrderingTerm(expr, order);
//	}

	private TDEngineExpression generateLimit(long l) {
		if (Randomly.getBoolean()) {
			return TDEngineConstant.createIntConstant(r.getLong(l, Long.MAX_VALUE));
		} else {
			return null;
		}
	}

	private TDEngineExpression generateWhereClauseThatContainsRowValue(List<TDEngineColumn> columns) {

		do {
			TDEngineExpression expr = new TDEngineExpressionGenerator(r).setUsedInWhere(true).setRowValue(rw).
					setColumns(columns).getRandomExpression();
			Optional<Boolean> boolVal = expr.getExpectedValue().asBoolean();
			if (boolVal.isPresent() && boolVal.get()) {
				return expr;
			}
		} while (true);
	}


}
