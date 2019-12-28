package lama.sqlite3.queries;

import java.util.ArrayList;
import java.util.List;

import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.ast.SQLite3SelectStatement.SelectType;
import lama.sqlite3.ast.SQLite3WindowFunction;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3FrameSpecExclude;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3FrameSpecKind;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecBetween;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm;
import lama.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm.SQLite3WindowFunctionFrameSpecTermKind;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Tables;

public class SQLite3RandomQuerySynthesizer {

	// TODO join clauses
	// TODO union, intersect
	public static SQLite3SelectStatement generate(SQLite3GlobalState globalState, int size) {
		Randomly r = globalState.getRandomly();
		SQLite3Schema s = globalState.getSchema();
		Tables targetTables = s.getRandomTableNonEmptyTables();
		List<SQLite3Expression> expressions = new ArrayList<>();
		SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState)
				.setColumns(s.getTables().getColumns());
		SQLite3ExpressionGenerator aggregateGen = new SQLite3ExpressionGenerator(globalState)
				.setColumns(s.getTables().getColumns()).allowAggregateFunctions();

		// SELECT
		SQLite3SelectStatement select = new SQLite3SelectStatement();
		// DISTINCT or ALL
		select.setSelectType(Randomly.fromOptions(SelectType.values()));
		for (int i = 0; i < size; i++) {
			if (Randomly.getBooleanWithSmallProbability()) {
				SQLite3Expression baseWindowFunction;
				boolean normalAggregateFunction = Randomly.getBoolean();
				if (!normalAggregateFunction) {
					baseWindowFunction = SQLite3WindowFunction.getRandom(targetTables.getColumns(), globalState);
				} else {
					baseWindowFunction = gen.getAggregateFunction(true);
					assert baseWindowFunction != null;
				}
				SQLite3WindowFunctionExpression windowFunction = new SQLite3WindowFunctionExpression(
						baseWindowFunction);
				if (Randomly.getBoolean() && normalAggregateFunction) {
					windowFunction.setFilterClause(gen.getRandomExpression());
				}
				if (Randomly.getBoolean()) {
					windowFunction.setOrderBy(gen.generateOrderingTerms());
				}
				if (Randomly.getBoolean()) {
					windowFunction.setPartitionBy(gen.getRandomExpressions(Randomly.smallNumber()));
				}
				if (Randomly.getBoolean()) {
					windowFunction.setFrameSpecKind(SQLite3FrameSpecKind.getRandom());
					SQLite3Expression windowFunctionTerm;
					if (Randomly.getBoolean()) {
						windowFunctionTerm = new SQLite3WindowFunctionFrameSpecTerm(
								Randomly.fromOptions(SQLite3WindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING,
										SQLite3WindowFunctionFrameSpecTermKind.CURRENT_ROW));
					} else if (Randomly.getBoolean()) {
						windowFunctionTerm = new SQLite3WindowFunctionFrameSpecTerm(gen.getRandomExpression(),
								SQLite3WindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
					} else {
						SQLite3WindowFunctionFrameSpecTerm left = getTerm(true, gen);
						SQLite3WindowFunctionFrameSpecTerm right = getTerm(false, gen);
						windowFunctionTerm = new SQLite3WindowFunctionFrameSpecBetween(left, right);
					}
					windowFunction.setFrameSpec(windowFunctionTerm);
					if (Randomly.getBoolean()) {
						windowFunction.setExclude(SQLite3FrameSpecExclude.getRandom());
					}
				}
				expressions.add(windowFunction);
			} else {
				expressions.add(aggregateGen.getRandomExpression());
			}
		}
		select.setFetchColumns(expressions);
		List<Table> tables = targetTables.getTables();
		if (Randomly.getBoolean()) {
			// JOIN ... (might remove tables)
			select.setJoinClauses(gen.getRandomJoinClauses(tables));
		}
		// FROM ...
		select.setFromList(SQLite3Common.getTableRefs(tables, s));
		// TODO: no values are referenced from this sub query yet
		if (Randomly.getBooleanWithSmallProbability()) {
			select.getFromList().add(SQLite3RandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1));
		}
		
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
			select.setOrderByClause(gen.generateOrderingTerms());
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

	private static SQLite3WindowFunctionFrameSpecTerm getTerm(boolean isLeftTerm, SQLite3ExpressionGenerator gen) {
		if (Randomly.getBoolean()) {
			SQLite3Expression expr = gen.getRandomExpression();
			SQLite3WindowFunctionFrameSpecTermKind kind = Randomly.fromOptions(
					SQLite3WindowFunctionFrameSpecTermKind.EXPR_FOLLOWING,
					SQLite3WindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
			return new SQLite3WindowFunctionFrameSpecTerm(expr, kind);
		} else if (Randomly.getBoolean()) {
			return new SQLite3WindowFunctionFrameSpecTerm(SQLite3WindowFunctionFrameSpecTermKind.CURRENT_ROW);
		} else {
			if (isLeftTerm) {
				return new SQLite3WindowFunctionFrameSpecTerm(SQLite3WindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING);
			} else {
				return new SQLite3WindowFunctionFrameSpecTerm(SQLite3WindowFunctionFrameSpecTermKind.UNBOUNDED_FOLLOWING);
			}
		}
	}


}
