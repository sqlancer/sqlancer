package lama.sqlite3.gen.ddl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lama.IgnoreMeException;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3SelectStatement;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.queries.SQLite3PivotedQuerySynthesizer;
import lama.sqlite3.queries.SQLite3RandomQuerySynthesizer;
import lama.sqlite3.schema.SQLite3Schema;

public class SQLite3ViewGenerator {

	public static Query dropView(SQLite3GlobalState globalState) {
		SQLite3Schema s = globalState.getSchema();
		StringBuilder sb = new StringBuilder("DROP VIEW ");
		sb.append(s.getRandomViewOrBailout().getName());
		return new QueryAdapter(sb.toString(), true);
	}

	public static Query generate(SQLite3GlobalState globalState) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
		}
		sb.append(" VIEW ");
		if (Randomly.getBoolean() || true) {
			sb.append(" IF NOT EXISTS ");
		}
		sb.append(SQLite3Common.getFreeViewName(globalState.getSchema()));
		List<String> errors = new ArrayList<>();
		errors.add("is circularly defined");
		errors.add("unsupported frame specification");
		if (Randomly.getBoolean()) {
			SQLite3PivotedQuerySynthesizer queryGen = new SQLite3PivotedQuerySynthesizer(globalState.getConnection(), globalState.getRandomly(), globalState);
			try {
				SQLite3SelectStatement q = queryGen.getQuery(globalState);
//			for (SQLite3Expression expr : q.getFetchColumns()) {
//				if (expr.getAffinity() != null || expr.getImplicitCollateSequence() != null || expr.getExplicitCollateSequence() != null) {
//					throw new IgnoreMeException();
//				}
//			}
				int size = q.getFetchColumns().size();
				columnNamesAs(sb, size);
				sb.append(SQLite3Visitor.asString(q));
				SQLite3PivotedQuerySynthesizer.addExpectedErrors(errors);
				return new QueryAdapter(sb.toString(), errors, true);
			} catch (AssertionError e) {
				throw new IgnoreMeException();
			}
		} else {
			int size = 1 + Randomly.smallNumber();
			columnNamesAs(sb, size);
			SQLite3SelectStatement randomQuery = SQLite3RandomQuerySynthesizer.generate(globalState, size);
			sb.append(SQLite3Visitor.asString(randomQuery));
			SQLite3PivotedQuerySynthesizer.addExpectedErrors(errors);
			return new QueryAdapter(sb.toString(), errors, true);
		}

	}

	private static void columnNamesAs(StringBuilder sb, int size) {
		sb.append("(");
		for (int i = 0; i < size; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(SQLite3Common.createColumnName(i));
		}
		sb.append(")");
		sb.append(" AS ");
	}

}
