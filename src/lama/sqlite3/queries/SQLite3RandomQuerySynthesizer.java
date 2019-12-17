package lama.sqlite3.queries;

import java.util.ArrayList;
import java.util.List;

import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.ast.SQLite3WindowFunction;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3RandomQuerySynthesizer {

	public static SQLite3SelectStatement generate(SQLite3GlobalState globalState, int size) {
		Randomly r = globalState.getRandomly();
		SQLite3Schema s = globalState.getSchema();
		Tables targetTables = s.getRandomTableNonEmptyTables();
		List<SQLite3Expression> expressions = new ArrayList<>();
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(r).setColumns(s.getTables().getColumns());
		SQLite3ExpressionGenerator aggregateGen = new SQLite3ExpressionGenerator(r).setColumns(s.getTables().getColumns()).allowAggregateFunctions();

		// SELECT
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		// DISTINCT or ALL
		select.setSelectType(Randomly.fromOptions(SelectType.values()));
		for (int i = 0; i < size; i++) {
			if (Randomly.getBoolean()) {
					SQLite3WindowFunction windowFunction = SQLite3WindowFunction.getRandom(targetTables.getColumns(), r);
					SQLite3Expression windowExpr = generateWindowFunction(targetTables.getColumns(), windowFunction, false /* TODO check */, r);
					expressions.add(windowExpr);
			} else {
				expressions.add(aggregateGen.getRandomExpression());
			}
		}
		select.setFetchColumns(expressions);
		// FROM ...
		select.setFromList(targetTables.getTables());
		// WHERE
		if (Randomly.getBoolean()) {
			select.setWhereClause(gen.getRandomExpression());
		}
		if (Randomly.getBoolean()) {
			// GROUP BY
			select.setGroupByClause(gen.getRandomExpressions(Randomly.smallNumber() + 1));
			if (Randomly.getBoolean()) {
				// HAVING
				select.setHavingClause(aggregateGen.getRandomExpression());
			}
		}
		if (Randomly.getBoolean()) {
			// ORDER BY
			select.setOrderByClause(SQLite3Common.getOrderBy(s.getTables().getColumns(), r));
		}
		if (Randomly.getBoolean()) {
			// LIMIT
			select.setLimitClause(SQLite3Constant.createIntConstant(r.getInteger()));
			if (Randomly.getBoolean()) {
				// OFFSET
				select.setOffsetClause(SQLite3Constant.createIntConstant(r.getInteger()));
			}
		}
		return select;
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


}
