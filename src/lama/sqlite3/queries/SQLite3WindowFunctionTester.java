package lama.sqlite3.queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lama.IgnoreMeException;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.ColumnName;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.ast.SQLite3WindowFunction;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3WindowFunctionTester {

	private static final int NOT_FOUND = -1;
	private SQLite3Schema s;
	private Randomly r;
	private Connection con;
	private final List<String> queries = new ArrayList<>();
	private final List<String> errors = new ArrayList<>();
	private SQLite3GlobalState globalState;

	public SQLite3WindowFunctionTester(SQLite3Schema s, Randomly r, Connection con, SQLite3StateToReproduce state,
			StateLogger logger, MainOptions options, SQLite3GlobalState globalState) {
		this.s = s;
		this.r = r;
		this.con = con;
		this.globalState = globalState;
		SQLite3Errors.addExpectedExpressionErrors(errors);
		SQLite3Errors.addMatchQueryErrors(errors);

		// aggregate
		errors.add("misuse of aggregate");
		errors.add("second argument to nth_value must be a positive integer");
		errors.add("no such table");
//		errors.add("no such index"); // INDEXED BY 
//		errors.add("no query solution"); // INDEXED BY
	}

	public void generateAndCheck() throws SQLException {
		queries.clear();
		Tables randomTable = s.getRandomTableNonEmptyTables();
		List<Column> columns = randomTable.getColumns();
		SQLite3Expression randomWhereCondition = getRandomWhereCondition(columns);
		List<SQLite3Expression> groupBys = Collections.emptyList(); // getRandomExpressions(columns);
		List<Table> tables = randomTable.getTables();
		List<Join> joinStatements = new ArrayList<>();
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		select.setGroupByClause(groupBys);
		ColumnName rowid = new ColumnName(
				new Column(tables.get(0).getName() + ".rowid", SQLite3DataType.INT, false, false, null), null);
		SQLite3WindowFunction windowFunction = SQLite3AggregateFunction.getRandom(randomTable.getColumns(), r);

		SQLite3Expression windowFunc = generateWindowFunction(columns, windowFunction, false, r);
		select.setFetchColumns(Arrays.asList(rowid, windowFunc));
		select.setFromTables(tables);
		select.setSelectType(SelectType.ALL);
		select.setJoinClauses(joinStatements);
		String totalString = SQLite3Visitor.asString(select);
		queries.add(totalString);
		QueryAdapter q = new QueryAdapter(totalString, errors);
		try (ResultSet rs = q.executeAndGet(con)) {
			if (rs == null) {
				throw new IgnoreMeException();
			}
			if (rs.next()) {
				System.out.println("rowid: " + rs.getInt(1));
				System.out.println("result: " + rs.getString(1));
			}
			rs.getStatement().close();
		}
	}

	private static SQLite3Expression generateWindowFunction(List<Column> columns, SQLite3Expression colName,
			boolean allowFilter, Randomly r) {
		StringBuilder sb = new StringBuilder();
		if (Randomly.getBoolean() && allowFilter) {
			appendFilter(columns, sb, r);
		}
		sb.append(" OVER ");
		sb.append("(");
		if (Randomly.getBoolean()) {
			appendPartitionBy(columns, sb, r);
		}
		if (Randomly.getBoolean()) {
			sb.append(SQLite3Common.getOrderByAsString(columns, r));
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("RANGE", "ROWS", "GROUPS"));
			sb.append(" ");
			switch (Randomly.fromOptions(FrameSpec.values())) {
			case BETWEEN:
				sb.append("BETWEEN");
				sb.append(" UNBOUNDED PRECEDING AND CURRENT ROW");
				break;
			case UNBOUNDED_PRECEDING:
				sb.append("UNBOUNDED PRECEDING");
				break;
			case CURRENT_ROW:
				sb.append("CURRENT ROW");
				break;
			}
//			sb.append(" BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
			if (Randomly.getBoolean()) {
				sb.append(" EXCLUDE ");
				// "CURRENT ROW", "GROUP"
				sb.append(Randomly.fromOptions("NO OTHERS", "TIES"));
			}
		}
		sb.append(")");
		colName = new SQLite3PostfixText(colName, sb.toString(), colName.getExpectedValue());
		return colName;
	}

	//

	private static void appendFilter(List<Column> columns, StringBuilder sb, Randomly r) {
		sb.append(" FILTER (WHERE ");
		sb.append(SQLite3Visitor.asString(new SQLite3ExpressionGenerator(r).setColumns(columns).getRandomExpression()));
		sb.append(")");
	}

	private static void appendPartitionBy(List<Column> columns, StringBuilder sb, Randomly r) {
		sb.append(" PARTITION BY ");
		for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			String orderingTerm;
			do {
				orderingTerm = SQLite3Common.getOrderingTerm(columns, r);
			} while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
			// TODO investigate
			sb.append(orderingTerm);
		}
	}

	private enum FrameSpec {
		BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
	}

	private SQLite3Expression getRandomWhereCondition(List<Column> columns) {
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(r).setColumns(columns).setGlobalState(globalState);
		// FIXME: enable match clause for multiple tables
//		if (randomTable.isVirtual()) {
//			gen.allowMatchClause();
//		}
		return gen.getRandomExpression();
	}

}
