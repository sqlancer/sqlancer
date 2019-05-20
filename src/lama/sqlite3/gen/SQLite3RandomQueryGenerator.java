package lama.sqlite3.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3Expression.Join;
import lama.sqlite3.ast.SQLite3Expression.Join.JoinType;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3RandomQueryGenerator {

	public static SQLite3SelectStatement generateRandomQuery(SQLite3Schema s, Randomly r) {
		Tables randomFromTables = s.getRandomTableNonEmptyTables();
		List<Table> tables = randomFromTables.getTables();
		SQLite3SelectStatement selectStatement = new SQLite3SelectStatement();
		selectStatement.setSelectType(Randomly.fromOptions(SQLite3SelectStatement.SelectType.values()));
		List<Column> columns = randomFromTables.getColumns();
		for (Table t : tables) {
			if (t.getRowid() != null) {
				columns.add(t.getRowid());
			}
		}
		
		List<Join> joinStatements = new ArrayList<>();
		for (int i = 1; i < tables.size(); i++) {
			SQLite3Expression joinClause =  SQLite3ExpressionGenerator.getRandomExpression(columns, false, r);
			Table table = Randomly.fromList(tables);
			tables.remove(table);
			JoinType options;
			if (tables.size() == 2) {
				// allow outer with arbitrary column order (see error: ON clause references
				// tables to its right)
				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS, JoinType.OUTER);
			} else {
				options = Randomly.fromOptions(JoinType.INNER, JoinType.CROSS);
			}
			Join j = new SQLite3Expression.Join(table, joinClause, options);
			joinStatements.add(j);
		}
		selectStatement.setJoinClauses(joinStatements);
		selectStatement.setFromTables(tables);
		
		List<Column> columnsWithoutRowid = columns.stream().filter(c -> !c.getName().matches("rowid"))
				.collect(Collectors.toList());
		List<Column> fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
		selectStatement.selectFetchColumns(fetchColumns);
		SQLite3Expression whereClause = SQLite3ExpressionGenerator.getRandomExpression(columns, false, r);
		selectStatement.setWhereClause(whereClause);
		return selectStatement;
//		List<SQLite3Expression> groupByClause = generateGroupByClause(columns);
//		selectStatement.setGroupByClause(groupByClause);
//		SQLite3Expression limitClause = generateLimit();
//		selectStatement.setLimitClause(limitClause);
//		if (limitClause != null) {
//			SQLite3Expression offsetClause = generateOffset();
//			selectStatement.setOffsetClause(offsetClause);
//		}
//		List<SQLite3Expression> orderBy = generateOrderBy(columns);
//		selectStatement.setOrderByClause(orderBy);
		
	}
	
}
